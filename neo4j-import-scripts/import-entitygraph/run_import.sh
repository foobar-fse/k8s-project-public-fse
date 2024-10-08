#!/bin/bash

neo4jPath=/home/neo/neo4j-community-4.3.6
dataPath=/home/neo/neo4j-import-graphframe-total-2


$neo4jPath/bin/neo4j-admin import --database=neo4j \
  --delimiter=";" --array-delimiter="|" --quote="'" --bad-tolerance=10000 \
  --skip-duplicate-nodes --skip-bad-relationships \
  --nodes=APIService=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/APIService.csv\
  --nodes=BlockAffinity=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/BlockAffinity.csv\
  --nodes=ClusterInformation=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ClusterInformation.csv\
  --nodes=ClusterRole=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ClusterRole.csv\
  --nodes=ClusterRoleBinding=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ClusterRoleBinding.csv\
  --nodes=ConfigMap=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ConfigMap.csv\
  --nodes=Configuration=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Configuration.csv\
  --nodes=ControllerRevision=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ControllerRevision.csv\
  --nodes=CronJob=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/CronJob.csv\
  --nodes=CustomResourceDefinition=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/CustomResourceDefinition.csv\
  --nodes=DaemonSet=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/DaemonSet.csv\
  --nodes=Deployment=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Deployment.csv\
  --nodes=DestinationRule=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/DestinationRule.csv\
  --nodes=Endpoints=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Endpoints.csv\
  --nodes=Event=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Event.csv\
  --nodes=FelixConfiguration=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/FelixConfiguration.csv\
  --nodes=Gateway=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Gateway.csv\
  --nodes=Group=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Group.csv\
  --nodes=HorizontalPodAutoscaler=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/HorizontalPodAutoscaler.csv\
  --nodes=IPAMBlock=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/IPAMBlock.csv\
  --nodes=IPAMHandle=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/IPAMHandle.csv\
  --nodes=IPPool=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/IPPool.csv\
  --nodes=Image=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Image.csv\
  --nodes=Ingress=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Ingress.csv\
  --nodes=Job=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Job.csv\
  --nodes=Lease=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Lease.csv\
  --nodes=LimitRange=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/LimitRange.csv\
  --nodes=MeshPolicy=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/MeshPolicy.csv\
  --nodes=Metric=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Metric.csv\
  --nodes=MutatingWebhookConfiguration=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/MutatingWebhookConfiguration.csv\
  --nodes=Namespace=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Namespace.csv\
  --nodes=Node=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Node.csv\
  --nodes=PersistentVolume=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PersistentVolume.csv\
  --nodes=PersistentVolumeClaim=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PersistentVolumeClaim.csv\
  --nodes=Pod=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Pod.csv\
  --nodes=PodAutoscaler=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PodAutoscaler.csv\
  --nodes=PodDisruptionBudget=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PodDisruptionBudget.csv\
  --nodes=PriorityClass=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PriorityClass.csv\
  --nodes=Prometheus=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Prometheus.csv\
  --nodes=PrometheusRule=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/PrometheusRule.csv\
  --nodes=ReplicaSet=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ReplicaSet.csv\
  --nodes=ResourceQuota=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ResourceQuota.csv\
  --nodes=Revision=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Revision.csv\
  --nodes=Role=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Role.csv\
  --nodes=RoleBinding=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/RoleBinding.csv\
  --nodes=Route=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Route.csv\
  --nodes=Secret=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Secret.csv\
  --nodes=ServerlessService=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ServerlessService.csv\
  --nodes=Service=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/Service.csv\
  --nodes=ServiceAccount=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ServiceAccount.csv\
  --nodes=ServiceMonitor=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ServiceMonitor.csv\
  --nodes=StatefulSet=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/StatefulSet.csv\
  --nodes=StorageClass=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/StorageClass.csv\
  --nodes=User=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/User.csv\
  --nodes=ValidatingWebhookConfiguration=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/ValidatingWebhookConfiguration.csv\
  --nodes=VirtualService=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/VirtualService.csv\
  --nodes=attributemanifest=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/attributemanifest.csv\
  --nodes=handler=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/handler.csv\
  --nodes=instance=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/instance.csv\
  --nodes=rule=$dataPath/headers/srcVertex/k8s-native_header.csv,$dataPath/nodes/srcVertex/rule.csv\
  --nodes=APISERVICE=$dataPath/headers/destVertex/APISERVICE_header.csv,$dataPath/nodes/destVertex/APISERVICE.csv\
  --nodes=ATTRIBUTEMANIFEST=$dataPath/headers/destVertex/ATTRIBUTEMANIFEST_header.csv,$dataPath/nodes/destVertex/ATTRIBUTEMANIFEST.csv\
  --nodes=BLOCKAFFINITY=$dataPath/headers/destVertex/BLOCKAFFINITY_header.csv,$dataPath/nodes/destVertex/BLOCKAFFINITY.csv\
  --nodes=CERTIFICATESIGNINGREQUEST=$dataPath/headers/destVertex/CERTIFICATESIGNINGREQUEST_header.csv,$dataPath/nodes/destVertex/CERTIFICATESIGNINGREQUEST.csv\
  --nodes=CLUSTERINFORMATION=$dataPath/headers/destVertex/CLUSTERINFORMATION_header.csv,$dataPath/nodes/destVertex/CLUSTERINFORMATION.csv\
  --nodes=CLUSTERROLE=$dataPath/headers/destVertex/CLUSTERROLE_header.csv,$dataPath/nodes/destVertex/CLUSTERROLE.csv\
  --nodes=CLUSTERROLEBINDING=$dataPath/headers/destVertex/CLUSTERROLEBINDING_header.csv,$dataPath/nodes/destVertex/CLUSTERROLEBINDING.csv\
  --nodes=COMPONENTSTATUS=$dataPath/headers/destVertex/COMPONENTSTATUS_header.csv,$dataPath/nodes/destVertex/COMPONENTSTATUS.csv\
  --nodes=CONFIGMAP=$dataPath/headers/destVertex/CONFIGMAP_header.csv,$dataPath/nodes/destVertex/CONFIGMAP.csv\
  --nodes=CONFIGURATION=$dataPath/headers/destVertex/CONFIGURATION_header.csv,$dataPath/nodes/destVertex/CONFIGURATION.csv\
  --nodes=CONTROLLERREVISION=$dataPath/headers/destVertex/CONTROLLERREVISION_header.csv,$dataPath/nodes/destVertex/CONTROLLERREVISION.csv\
  --nodes=CRONJOB=$dataPath/headers/destVertex/CRONJOB_header.csv,$dataPath/nodes/destVertex/CRONJOB.csv\
  --nodes=CUSTOMRESOURCEDEFINITION=$dataPath/headers/destVertex/CUSTOMRESOURCEDEFINITION_header.csv,$dataPath/nodes/destVertex/CUSTOMRESOURCEDEFINITION.csv\
  --nodes=DAEMONSET=$dataPath/headers/destVertex/DAEMONSET_header.csv,$dataPath/nodes/destVertex/DAEMONSET.csv\
  --nodes=DEPLOYMENT=$dataPath/headers/destVertex/DEPLOYMENT_header.csv,$dataPath/nodes/destVertex/DEPLOYMENT.csv\
  --nodes=DESTINATIONRULE=$dataPath/headers/destVertex/DESTINATIONRULE_header.csv,$dataPath/nodes/destVertex/DESTINATIONRULE.csv\
  --nodes=ENDPOINTS=$dataPath/headers/destVertex/ENDPOINTS_header.csv,$dataPath/nodes/destVertex/ENDPOINTS.csv\
  --nodes=EVENT=$dataPath/headers/destVertex/EVENT_header.csv,$dataPath/nodes/destVertex/EVENT.csv\
  --nodes=FELIXCONFIGURATION=$dataPath/headers/destVertex/FELIXCONFIGURATION_header.csv,$dataPath/nodes/destVertex/FELIXCONFIGURATION.csv\
  --nodes=GATEWAY=$dataPath/headers/destVertex/GATEWAY_header.csv,$dataPath/nodes/destVertex/GATEWAY.csv\
  --nodes=HANDLER=$dataPath/headers/destVertex/HANDLER_header.csv,$dataPath/nodes/destVertex/HANDLER.csv\
  --nodes=HORIZONTALPODAUTOSCALER=$dataPath/headers/destVertex/HORIZONTALPODAUTOSCALER_header.csv,$dataPath/nodes/destVertex/HORIZONTALPODAUTOSCALER.csv\
  --nodes=IMAGE=$dataPath/headers/destVertex/IMAGE_header.csv,$dataPath/nodes/destVertex/IMAGE.csv\
  --nodes=INSTANCE=$dataPath/headers/destVertex/INSTANCE_header.csv,$dataPath/nodes/destVertex/INSTANCE.csv\
  --nodes=IPAMBLOCK=$dataPath/headers/destVertex/IPAMBLOCK_header.csv,$dataPath/nodes/destVertex/IPAMBLOCK.csv\
  --nodes=IPAMHANDLE=$dataPath/headers/destVertex/IPAMHANDLE_header.csv,$dataPath/nodes/destVertex/IPAMHANDLE.csv\
  --nodes=IPPOOL=$dataPath/headers/destVertex/IPPOOL_header.csv,$dataPath/nodes/destVertex/IPPOOL.csv\
  --nodes=JOB=$dataPath/headers/destVertex/JOB_header.csv,$dataPath/nodes/destVertex/JOB.csv\
  --nodes=LEASE=$dataPath/headers/destVertex/LEASE_header.csv,$dataPath/nodes/destVertex/LEASE.csv\
  --nodes=LIMITRANGE=$dataPath/headers/destVertex/LIMITRANGE_header.csv,$dataPath/nodes/destVertex/LIMITRANGE.csv\
  --nodes=MESHPOLICY=$dataPath/headers/destVertex/MESHPOLICY_header.csv,$dataPath/nodes/destVertex/MESHPOLICY.csv\
  --nodes=METRIC=$dataPath/headers/destVertex/METRIC_header.csv,$dataPath/nodes/destVertex/METRIC.csv\
  --nodes=MUTATINGWEBHOOKCONFIGURATION=$dataPath/headers/destVertex/MUTATINGWEBHOOKCONFIGURATION_header.csv,$dataPath/nodes/destVertex/MUTATINGWEBHOOKCONFIGURATION.csv\
  --nodes=NAMESPACE=$dataPath/headers/destVertex/NAMESPACE_header.csv,$dataPath/nodes/destVertex/NAMESPACE.csv\
  --nodes=NFS=$dataPath/headers/destVertex/NFS_header.csv,$dataPath/nodes/destVertex/NFS.csv\
  --nodes=NODE=$dataPath/headers/destVertex/NODE_header.csv,$dataPath/nodes/destVertex/NODE.csv\
  --nodes=PERSISTENTVOLUME=$dataPath/headers/destVertex/PERSISTENTVOLUME_header.csv,$dataPath/nodes/destVertex/PERSISTENTVOLUME.csv\
  --nodes=PERSISTENTVOLUMECLAIM=$dataPath/headers/destVertex/PERSISTENTVOLUMECLAIM_header.csv,$dataPath/nodes/destVertex/PERSISTENTVOLUMECLAIM.csv\
  --nodes=POD=$dataPath/headers/destVertex/POD_header.csv,$dataPath/nodes/destVertex/POD.csv\
  --nodes=PODAUTOSCALER=$dataPath/headers/destVertex/PODAUTOSCALER_header.csv,$dataPath/nodes/destVertex/PODAUTOSCALER.csv\
  --nodes=PODDISRUPTIONBUDGET=$dataPath/headers/destVertex/PODDISRUPTIONBUDGET_header.csv,$dataPath/nodes/destVertex/PODDISRUPTIONBUDGET.csv\
  --nodes=PRIORITYCLASS=$dataPath/headers/destVertex/PRIORITYCLASS_header.csv,$dataPath/nodes/destVertex/PRIORITYCLASS.csv\
  --nodes=PROMETHEUS=$dataPath/headers/destVertex/PROMETHEUS_header.csv,$dataPath/nodes/destVertex/PROMETHEUS.csv\
  --nodes=PROMETHEUSRULE=$dataPath/headers/destVertex/PROMETHEUSRULE_header.csv,$dataPath/nodes/destVertex/PROMETHEUSRULE.csv\
  --nodes=REPLICASET=$dataPath/headers/destVertex/REPLICASET_header.csv,$dataPath/nodes/destVertex/REPLICASET.csv\
  --nodes=RESOURCEQUOTA=$dataPath/headers/destVertex/RESOURCEQUOTA_header.csv,$dataPath/nodes/destVertex/RESOURCEQUOTA.csv\
  --nodes=REVISION=$dataPath/headers/destVertex/REVISION_header.csv,$dataPath/nodes/destVertex/REVISION.csv\
  --nodes=ROLE=$dataPath/headers/destVertex/ROLE_header.csv,$dataPath/nodes/destVertex/ROLE.csv\
  --nodes=ROLEBINDING=$dataPath/headers/destVertex/ROLEBINDING_header.csv,$dataPath/nodes/destVertex/ROLEBINDING.csv\
  --nodes=ROUTE=$dataPath/headers/destVertex/ROUTE_header.csv,$dataPath/nodes/destVertex/ROUTE.csv\
  --nodes=RULE=$dataPath/headers/destVertex/RULE_header.csv,$dataPath/nodes/destVertex/RULE.csv\
  --nodes=SECRET=$dataPath/headers/destVertex/SECRET_header.csv,$dataPath/nodes/destVertex/SECRET.csv\
  --nodes=SERVERLESSSERVICE=$dataPath/headers/destVertex/SERVERLESSSERVICE_header.csv,$dataPath/nodes/destVertex/SERVERLESSSERVICE.csv\
  --nodes=SERVICE=$dataPath/headers/destVertex/SERVICE_header.csv,$dataPath/nodes/destVertex/SERVICE.csv\
  --nodes=SERVICEACCOUNT=$dataPath/headers/destVertex/SERVICEACCOUNT_header.csv,$dataPath/nodes/destVertex/SERVICEACCOUNT.csv\
  --nodes=SERVICEMONITOR=$dataPath/headers/destVertex/SERVICEMONITOR_header.csv,$dataPath/nodes/destVertex/SERVICEMONITOR.csv\
  --nodes=STATEFULSET=$dataPath/headers/destVertex/STATEFULSET_header.csv,$dataPath/nodes/destVertex/STATEFULSET.csv\
  --nodes=STORAGECLASS=$dataPath/headers/destVertex/STORAGECLASS_header.csv,$dataPath/nodes/destVertex/STORAGECLASS.csv\
  --nodes=VALIDATINGWEBHOOKCONFIGURATION=$dataPath/headers/destVertex/VALIDATINGWEBHOOKCONFIGURATION_header.csv,$dataPath/nodes/destVertex/VALIDATINGWEBHOOKCONFIGURATION.csv\
  --nodes=VIRTUALSERVICE=$dataPath/headers/destVertex/VIRTUALSERVICE_header.csv,$dataPath/nodes/destVertex/VIRTUALSERVICE.csv\
  --nodes=atomic=$dataPath/headers/destVertex/atomic_header.csv,$dataPath/nodes/destVertex/atomic.csv\
  --nodes=container=$dataPath/headers/destVertex/container_header.csv,$dataPath/nodes/destVertex/container.csv\
  --nodes=hostPath=$dataPath/headers/destVertex/hostPath_header.csv,$dataPath/nodes/destVertex/hostPath.csv\
  --nodes=image=$dataPath/headers/destVertex/image_header.csv,$dataPath/nodes/destVertex/image.csv\
  --nodes=nfs=$dataPath/headers/destVertex/nfs_header.csv,$dataPath/nodes/destVertex/nfs.csv\
  --relationships=$dataPath/headers/edge/edge_header.csv,$dataPath/relations/edge.csv
