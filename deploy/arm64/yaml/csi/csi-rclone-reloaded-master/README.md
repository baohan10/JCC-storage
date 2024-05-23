# CSI rclone mount plugin

Fork of https://github.com/wunderio/csi-rclone that is a bit slow with merging PRs

Differences with that fork:

- Everything is under kube-system namespace
- Allow specifying of secrets for PV
- StorageClass is no longer namespaced

This project implements Container Storage Interface (CSI) plugin that allows using [rclone mount](https://rclone.org/) as storage backend. Rclone mount points and [parameters](https://rclone.org/commands/rclone_mount/) can be configured using Secret or PersistentVolume volumeAttibutes.

## Kubernetes cluster compatability

Works (tested):

- `deploy/kubernetes/1.19`: K8S>= 1.19.x (due to storage.k8s.io/v1 CSIDriver API)
- `deploy/kubernetes/1.13`: K8S 1.13.x - 1.21.x (storage.k8s.io/v1beta1 CSIDriver API)

Does not work:

- v1.12.7-gke.10, driver name csi-rclone not found in the list of registered CSI drivers

## Installing CSI driver to kubernetes cluster

TLDR: ` kubectl apply -f deploy/kubernetes/1.19` (or `deploy/kubernetes/1.13` for older version) to get the CSI setup

### Example: Adding Dropbox through rclone

The easiest way to use this is to specify your rclone configuration inside the PV:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: rclone-dropbox
  labels:
    name: rclone-dropbox
spec:
  accessModes:
    - ReadWriteMany
  capacity:
    storage: 10Gi
  storageClassName: rclone
  csi:
    driver: csi-rclone
    volumeHandle: rclone-dropbox-data-id
    volumeAttributes:
      remote: "dropbox"
      remotePath: ""
      configData: |
        [dropbox]
        type = dropbox
        client_id = xxx
        client_secret = xxx
        token = {"access_token":"xxx","token_type":"bearer","refresh_token":"xxx","expiry":"xxx"}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rclone-dropbox
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
  storageClassName: rclone
  selector:
    matchLabels:
      name: rclone-dropbox
```

(to get access token, setup Dropbox locally with rclone first, then copy whatever `rclone config show` gives you)

### Example: S3 storage without direct rclone configuration

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: rclone-wasabi
  labels:
    name: rclone-wasabi
spec:
  accessModes:
    - ReadWriteMany
  capacity:
    storage: 1000Gi
  storageClassName: rclone
  csi:
    driver: csi-rclone
    volumeHandle: data-id
    volumeAttributes:
      remote: "bucketname"
      remotePath: ""
      s3-provider: "Wasabi"
      s3-endpoint: "https://s3.ap-southeast-1.wasabisys.com"
      s3-access-key-id: "xxx"
      s3-secret-access-key: "xxx"
---
<pvc manifest here>
```

### Example: Using a secret (thanks to [wunderio/csi-rclone#7](https://github.com/wunderio/csi-rclone/pull/7))

_Note:_ secrets act as defaults, you can still override keys in your PV definitions.

_Note 2_: Use `secret-rclone` as global default for when there are no secrets defined, for example if you always want the same S3 credentials across your PVs

_Note 3_: Secrets need to be in the same namespace as the csi controller, so if you used the default of this repository, add it to `kube-system`

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-secret
  namespace: kube-system # <-- secret needs to be in kube-system namespace, same as CSI controller
type: Opaque
stringData:
  remote: "my-s3"
  remotePath: "projectname"
  configData: |
    [my-s3]
    type = s3
    provider = Minio
    access_key_id = ACCESS_KEY_ID
    secret_access_key = SECRET_ACCESS_KEY
    endpoint = http://minio-release.default:9000
```

Then specify it into the PV:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: rclone-dropbox
  labels:
    name: rclone-dropbox
spec:
  accessModes:
    - ReadWriteMany
  capacity:
    storage: 10Gi
  storageClassName: rclone
  csi:
    driver: csi-rclone
    volumeHandle: rclone-dropbox-data-id
    volumeAttributes:
      secretName: "my-secret"
```

## Debugging & logs

- After creating a pod, if something goes wrong you should be able to see it using `kubectl describe <pod>`
- Check logs of the controller: `kubectl logs -f -l app=csi-nodeplugin-rclone --namespace kube-system -c rclone`

## Building plugin and creating image

Current code is referencing projects repository on github.com. If you fork the repository, you have to change go includes in several places (use search and replace).

1. First push the changed code to remote. The build will use paths from `pkg/` directory.

2. Build the plugin

```
make plugin
```

3. Build the container and inject the plugin into it.

```
make container
```

4. Change docker.io account in `Makefile` and use `make push` to push the image to remote.

```
make push
```

## Changelog

See [CHANGELOG.txt](CHANGELOG.txt)
