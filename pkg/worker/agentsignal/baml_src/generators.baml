generator target {
    output_type "go"
    output_dir "../"
    version "0.220.0"
    on_generate "bash \"$(git rev-parse --show-toplevel)/bin/patch-baml-runtime.sh\" && gofmt -w . && goimports -w ."
    client_package_name "github.com/beam-cloud/airstore/pkg/worker/agentsignal"
}
