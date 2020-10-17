module github.com/retailnext/cassandrabackup

require (
	cloud.google.com/go/storage v1.12.0
	github.com/aws/aws-sdk-go v1.35.7
	github.com/go-test/deep v1.0.6
	github.com/gocql/gocql v0.0.0-20200926162733-393f0c961220
	github.com/golang/snappy v0.0.2 // indirect
	github.com/mailru/easyjson v0.7.6
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/common v0.14.0 // indirect
	github.com/prometheus/procfs v0.2.0 // indirect
	github.com/retailnext/writefile v0.1.0
	github.com/stretchr/testify v1.5.1 // indirect
	go.etcd.io/bbolt v1.3.5
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201002170205-7f63de1d35b0
	golang.org/x/sys v0.0.0-20201009025420-dfb3f7c4e634 // indirect
	google.golang.org/api v0.32.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.3.0
)

go 1.13
