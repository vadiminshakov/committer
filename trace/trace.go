package trace

import (
	zipkin "github.com/openzipkin/zipkin-go"
	httpreporter "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/pkg/errors"
)

func Tracer(serviceName, hostPort string) (*zipkin.Tracer, error) {
	reporter := httpreporter.NewReporter("http://localhost:9411/api/v2/spans")
	//defer reporter.Close()
	// create our local service endpoint
	endpoint, err := zipkin.NewEndpoint(serviceName, hostPort)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create local endpoint")
	}
	// initialize our tracer
	tracer, err := zipkin.NewTracer(reporter, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create tracer")
	}

	return tracer, nil
}
