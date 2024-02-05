# grid5000_deployment

Manage [Kadeploy](http://kadeploy3.gforge.inria.fr) deployments on Grid'5000.

## Exemple Usage

```hcl
resource "grid5000_deployment" "my_deployment" {
  site        = "rennes"
  environment = "debian10-x64-base"
  nodes       = grid5000_job.my_job.assigned_nodes
  key         = file("~/.ssh/id_rsa.pub")
}
```

## Argument Reference

* `site` - (Required) A grid'5000 site.
* `environment` - (Required) Environment name.
* `nodes` - (Required) Nodes list to deploy.
* `key` - (Optional) SSH pubkey to connect on deployed nodes.
* `partition_number` - (Optional, int) Partition number of the primary disk where environment will be deployed.
* `vlan` - (Optional, int) Vlan number to set nodes after deployment.

## Attribute Reference

* `state` - Deployment state.