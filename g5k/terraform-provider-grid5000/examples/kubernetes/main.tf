module "k8s_cluster" {
    source = "pmorillon/k8s-cluster/grid5000"
    version = "~> 0.0.1"
    nodes_count="4"
    site = "grenoble"
    nodes_selector = "{cluster = 'dahu'}"
    walltime = "2"
}
