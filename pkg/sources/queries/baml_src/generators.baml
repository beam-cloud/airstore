generator target {
    output_type "go"
    output_dir "../"
    version "0.218.1"

    // 'baml-cli generate' will run this after generating go code
    // This command will be run from within $output_dir/baml_client
    on_generate "gofmt -w . && goimports -w ."

    // Go packages name as specified in go.mod
    // We need this to generate correct imports in the generated baml_client
    client_package_name "github.com/beam-cloud/airstore/pkg/sources/queries"
}
