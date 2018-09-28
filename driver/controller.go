/*
Copyright 2018 DigitalOcean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/profitbricks/profitbricks-sdk-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

const (
	defaultVolumeSizeInGB = 16 * GB

	createdByPB = "Created by ProfitBricks CSI driver"
)

var (
	// PB currently only support a single node to be attached to a single node
	// in read/write mode. This corresponds to `accessModes.ReadWriteOnce` in a
	// PVC resource on Kubernets
	supportedAccessMode = &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}
)

// CreateVolume creates a new volume from the given request. The function is
// idempotent.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}

	if req.VolumeCapabilities == nil || len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	if req.AccessibilityRequirements != nil {
		for _, t := range req.AccessibilityRequirements.Requisite {
			region, ok := t.Segments["region"]
			if !ok {
				continue // nothing to do
			}

			if region != d.region {
				return nil, status.Errorf(codes.ResourceExhausted, "volume can be only created in region: %q, got: %q", d.region, region)

			}
		}
	}

	size, err := extractStorage(req.CapacityRange)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeName := req.Name

	ll := d.log.WithFields(logrus.Fields{
		"volume_name":             volumeName,
		"storage_size_giga_bytes": size / GB,
		"method":                  "create_volume",
		"volume_capabilities":     req.VolumeCapabilities,
	})
	ll.Info("create volume called")

	// get volume first, if it's created do no thing
	// TODO Add filter to ListVolumes
	volumesResponse, err := d.pbClient.ListVolumes(d.dcID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	filterVolume := func(v profitbricks.Volume) bool { return v.Properties.Name == volumeName }

	volumes := choose(volumesResponse.Items, filterVolume)

	// volume already exist, do nothing
	if len(volumes) != 0 {
		if len(volumes) > 1 {
			return nil, fmt.Errorf("fatal issue: duplicate volume %q exists", volumeName)
		}
		vol := volumes[0]

		if int64(vol.Properties.Size*GB) != size {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("invalid option requested size: %d", size))
		}

		ll.Info("volume already created")
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				Id:            vol.ID,
				CapacityBytes: int64(vol.Properties.Size) * GB,
			},
		}, nil
	}

	// TODO int64 to int32 problem?
	volumeReq := profitbricks.Volume{
		Properties: profitbricks.VolumeProperties{
			Name: volumeName,
			Size: int(size / GB),
			Type: "HDD",
			LicenceType: "OTHER",
		},
	}

	if !validateCapabilities(req.VolumeCapabilities) {
		return nil, status.Error(codes.AlreadyExists, "invalid volume capabilities requested. Only SINGLE_NODE_WRITER is supported ('accessModes.ReadWriteOnce' on Kubernetes)")
	}

	ll.Info("checking volume limit")
	if err := d.checkLimit(ctx); err != nil {
		return nil, err
	}

	ll.WithField("volume_req", volumeReq).Info("creating volume")
	vol, err := d.pbClient.CreateVolume(d.dcID, volumeReq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			Id:            vol.ID,
			CapacityBytes: size,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{
						"dcID": d.dcID,
					},
				},
			},
		},
	}

	ll.WithField("response", resp).Info("volume created")
	return resp, nil
}

// DeleteVolume deletes the given volume. The function is idempotent.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteVolume Volume ID must be provided")
	}

	ll := d.log.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"method":    "delete_volume",
	})
	ll.Info("delete volume called")

	resp, err := d.pbClient.DeleteVolume(d.dcID, req.VolumeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			// we assume it's deleted already for idempotency
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, err
	}

	ll.WithField("response", resp).Info("volume is deleted")
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches the given volume to the node
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}

	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Node ID must be provided")
	}

	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume capability must be provided")
	}

	if req.Readonly {
		// TODO(arslan): we should return codes.InvalidArgument, but the CSI
		// test fails, because according to the CSI Spec, this flag cannot be
		// changed on the same volume. However we don't use this flag at all,
		// as there are no `readonly` attachable volumes.
		return nil, status.Error(codes.AlreadyExists, "read only Volumes are not supported")
	}

	ll := d.log.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"node_id":   req.NodeId,
		"method":    "controller_publish_volume",
	})
	ll.Info("controller publish volume called")

	// check if volume exist before trying to attach it
	volume, err := d.pbClient.GetVolume(d.dcID, req.VolumeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
		}
		return nil, err
	}

	// check if droplet exist before trying to attach the volume to the droplet
	server, err := d.pbClient.GetServer(d.dcID, req.NodeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			return nil, status.Errorf(codes.NotFound, "server %q not found", req.NodeId)
		}
		return nil, err
	}

	// don't do anything if attached
	for _, volume := range server.Entities.Volumes.Items {
		if strings.ToLower(volume.ID) == strings.ToLower(req.VolumeId) {
			ll.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"node_id":   req.NodeId,
			}).Warn("assuming volume is attached already")
			return &csi.ControllerPublishVolumeResponse{}, nil
		}
	}

	// TODO null != 0
	if volume.Properties.DeviceNumber != 0 {
		ll.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"node_id":   req.NodeId,
		}).Warn("server is not able to attach the volume")
		// sending an abort makes sure the csi-attacher retries with the next backoff tick
		return nil, status.Errorf(codes.Aborted, "volume %q couldn't be attached, because already attached to other server",
			req.VolumeId)
	}

	attachResp, err := d.pbClient.AttachVolume(d.dcID, req.NodeId, req.VolumeId)
	if err != nil {
		return nil, err
	}

	ll.Info("waiting until volume is attached")
	if err := d.waitRequest(ctx, attachResp.Headers.Get("Location")); err != nil {
		return nil, err
	}

	ll.Info("volume is attached")
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume deattaches the given volume from the node
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}

	ll := d.log.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"node_id":   req.NodeId,
		"method":    "controller_unpublish_volume",
	})
	ll.Info("controller unpublish volume called")

	// check if volume exist before trying to detach it
	volume, err := d.pbClient.GetVolume(d.dcID, req.VolumeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			// assume it's detached
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, err
	}

	// check if droplet exist before trying to detach the volume to the droplet
	server, err := d.pbClient.GetServer(d.dcID, req.NodeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			return nil, status.Errorf(codes.NotFound, "server %q not found", req.NodeId)
		}
		return nil, err
	}

	// don't do anything if deattached
	foundVolume := false
	for _, volume := range server.Entities.Volumes.Items {
		if strings.ToLower(volume.ID) == strings.ToLower(req.VolumeId) {
			foundVolume = true
			break
		}
	}

	if !foundVolume {
		ll.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"node_id":   req.NodeId,
		}).Warn("assuming volume is detached already")
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// TODO: nil != 0
	if volume.Properties.DeviceNumber == 0 {
		ll.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"node_id":   req.NodeId,
		}).Warn("assuming volume is detached already")
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	attachResp, err := d.pbClient.DetachVolume(d.dcID, req.NodeId, req.VolumeId)
	if err != nil {
		return nil, err
	}

	ll.Info("waiting until volume is detached")
	if err := d.waitRequest(ctx, attachResp.Get("Location")); err != nil {
		return nil, err
	}

	ll.Info("volume is detached")
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities checks whether the volume capabilities requested
// are supported.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities Volume ID must be provided")
	}

	if req.VolumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities Volume Capabilities must be provided")
	}

	ll := d.log.WithFields(logrus.Fields{
		"volume_id":              req.VolumeId,
		"volume_capabilities":    req.VolumeCapabilities,
		"accessible_topology":    req.AccessibleTopology,
		"supported_capabilities": supportedAccessMode,
		"method":                 "validate_volume_capabilities",
	})
	ll.Info("validate volume capabilities called")

	// check if volume exist before trying to validate it it
	_, err := d.pbClient.GetVolume(d.dcID, req.VolumeId)
	if err != nil {
		apiError, ok := err.(profitbricks.ApiError)
		if !ok {
			return nil, err
		}

		if apiError.HttpStatusCode() == http.StatusNotFound {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
		}
		return nil, err
	}

	if req.AccessibleTopology != nil {
		for _, t := range req.AccessibleTopology {
			region, ok := t.Segments["region"]
			if !ok {
				continue // nothing to do
			}

			if region != d.region {
				// return early if a different region is expected
				ll.WithField("supported", false).Info("supported capabilities")
				return &csi.ValidateVolumeCapabilitiesResponse{
					Supported: false,
				}, nil
			}
		}
	}

	// if it's not supported (i.e: wrong region), we shouldn't override it
	resp := &csi.ValidateVolumeCapabilitiesResponse{
		Supported: validateCapabilities(req.VolumeCapabilities),
	}

	ll.WithField("supported", resp.Supported).Info("supported capabilities")
	return resp, nil
}

// ListVolumes returns a list of all requested volumes
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	var err error

	ll := d.log.WithFields(logrus.Fields{
		"req_starting_token": req.StartingToken,
		"method":             "list_volumes",
	})
	ll.Info("list volumes called")

	vols, err := d.pbClient.ListVolumes(d.dcID)
	if err != nil {
		return nil, err
	}

	var entries []*csi.ListVolumesResponse_Entry
	for _, vol := range vols.Items {
		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				Id:            vol.ID,
				CapacityBytes: int64(vol.Properties.Size * GB),
			},
		})
	}

	// TODO(arslan): check that the NextToken logic works fine, might be racy
	resp := &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: "1",
	}

	ll.WithField("response", resp).Info("volumes listed")
	return resp, nil
}

// GetCapacity returns the capacity of the storage pool
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	// TODO(arslan): check if we can provide this information somehow
	d.log.WithFields(logrus.Fields{
		"params": req.Parameters,
		"method": "get_capacity",
	}).Warn("get capacity is not implemented")
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	newCap := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
	}

	// TODO(arslan): checkout if the capabilities are worth supporting
	var caps []*csi.ControllerServiceCapability
	for _, cap := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,

		// TODO(arslan): enable once snapshotting is supported
		// csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		// csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	} {
		caps = append(caps, newCap(cap))
	}

	resp := &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}

	d.log.WithFields(logrus.Fields{
		"response": resp,
		"method":   "controller_get_capabilities",
	}).Info("controller get capabilities called")
	return resp, nil
}

// CreateSnapshot will be called by the CO to create a new snapshot from a
// source volume on behalf of a user.
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	d.log.WithFields(logrus.Fields{
		"req":    req,
		"method": "create_snapshot",
	}).Warn("create snapshot is not implemented")
	return nil, status.Error(codes.Unimplemented, "")
}

// DeleteSnapshost will be called by the CO to delete a snapshot.
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	d.log.WithFields(logrus.Fields{
		"req":    req,
		"method": "delete_snapshot",
	}).Warn("delete snapshot is not implemented")
	return nil, status.Error(codes.Unimplemented, "")
}

// ListSnapshots returns the information about all snapshots on the storage
// system within the given parameters regardless of how they were created.
// ListSnapshots shold not list a snapshot that is being created but has not
// been cut successfully yet.
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	d.log.WithFields(logrus.Fields{
		"req":    req,
		"method": "list_snapshots",
	}).Warn("list snapshots is not implemented")
	return nil, status.Error(codes.Unimplemented, "")
}

// extractStorage extracts the storage size in GB from the given capacity
// range. If the capacity range is not satisfied it returns the default volume
// size.
func extractStorage(capRange *csi.CapacityRange) (int64, error) {
	if capRange == nil {
		return defaultVolumeSizeInGB, nil
	}

	if capRange.RequiredBytes == 0 && capRange.LimitBytes == 0 {
		return defaultVolumeSizeInGB, nil
	}

	minSize := capRange.RequiredBytes

	// limitBytes might be zero
	maxSize := capRange.LimitBytes
	if capRange.LimitBytes == 0 {
		maxSize = minSize
	}

	if minSize == maxSize {
		return minSize, nil
	}

	return 0, errors.New("requiredBytes and LimitBytes are not the same")
}

// waitRequest waits until the given request is completed
func (d *Driver) waitRequest(ctx context.Context, request_id string) error {
	ll := d.log.WithFields(logrus.Fields{
		"request_id": request_id,
	})

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// TODO(arslan): use backoff in the future
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			request, err := d.pbClient.GetRequestStatus(request_id)
			if err != nil {
				ll.WithError(err).Info("waiting for request errored")
				continue
			}
			ll.WithField("request_status", request.Metadata.Status).Info("request received")

			if request.Metadata.Status == "DONE" {
				ll.Info("request completed")
				return nil
			}

			if request.Metadata.Status == "FAILED" {
				continue
			}
		case <-ctx.Done():
			return fmt.Errorf("timeout occured waiting for request id: %q", request_id)
		}
	}
}

// checkLimit checks whether the user hit their volume limit to ensure.
func (d *Driver) checkLimit(ctx context.Context) error {
	// only one provisioner runs, we can make sure to prevent burst creation
	d.readyMu.Lock()
	defer d.readyMu.Unlock()

	_, err := d.pbClient.GetContractResources()
	if err != nil {
		return status.Errorf(codes.Internal,
			"couldn't get account information to check volume limit: %s", err.Error())
	}
	// TODO
	// contract.Properties.{SsdLimitPerContract,HddLimitPerContract}

	return nil
}

// validateCapabilities validates the requested capabilities. It returns false
// if it doesn't satisfy the currently supported modes of ProfitBricks Block
// Storage
func validateCapabilities(caps []*csi.VolumeCapability) bool {
	vcaps := []*csi.VolumeCapability_AccessMode{supportedAccessMode}

	hasSupport := func(mode csi.VolumeCapability_AccessMode_Mode) bool {
		for _, m := range vcaps {
			if mode == m.Mode {
				return true
			}
		}
		return false
	}

	supported := false
	for _, cap := range caps {
		if hasSupport(cap.AccessMode.Mode) {
			supported = true
		} else {
			// we need to make sure all capabilities are supported. Revert back
			// in case we have a cap that is supported, but is invalidated now
			supported = false
		}
	}

	return supported
}
