generator target {
    output_type "go"
    output_dir "../"
    version "0.218.1"
    on_generate "gofmt -w . && goimports -w ."
    client_package_name "github.com/beam-cloud/airstore/pkg/worker/agentsignal"
}
