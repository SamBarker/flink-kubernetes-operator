package org.apache.flink.kubernetes.operator.metrics;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.http.HttpRequest;
import io.fabric8.kubernetes.client.http.HttpResponse;
import io.fabric8.kubernetes.client.http.Interceptor;
import io.fabric8.kubernetes.client.http.StandardHttpRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

class KubernetesClientMetricsFabric8InterceptorTest {

    private static final String NAMESPACE = "test-op-ns";
    private static final String NAME = "test-op-name";
    private static final String HOST = "test-op-host";

    private KubernetesClientMetrics kubernetesClientMetrics;
    private StandardHttpRequest.Builder builder;
    private Interceptor.RequestTags emptyTags;

    @BeforeEach
    void setUp() {
        TestingMetricRegistry registry = TestingMetricRegistry.builder().build();
        KubernetesOperatorMetricGroup metricGroup = KubernetesOperatorMetricGroup.create(
                registry, new Configuration(), NAMESPACE, NAME, HOST);
        kubernetesClientMetrics = new KubernetesClientMetrics(metricGroup, FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
        builder = new StandardHttpRequest.Builder();
        emptyTags = new Interceptor.RequestTags() {
            @Override
            public <T> T getTag(Class<T> aClass) {
                return null;
            }
        };
    }

    @Test
    void shouldCountPostRequest() {
        // Given
        final HttpRequest postRequest = builder.post("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter()).extracting(Counter::getCount).isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("POST")).extracting(Counter::getCount).isEqualTo(1L);
    }

    @Test
    void shouldCountDeleteRequest() {
        // Given
        final HttpRequest postRequest = builder.delete("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter()).extracting(Counter::getCount).isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("DELETE")).extracting(Counter::getCount).isEqualTo(1L);
    }

    @Test
    void shouldCountPatchRequest() {
        // Given
        final HttpRequest postRequest = builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);

        // Then
        assertThat(kubernetesClientMetrics.getRequestCounter()).extracting(Counter::getCount).isEqualTo(1L);
        assertThat(kubernetesClientMetrics.getRequestMethodCounter("PATCH")).extracting(Counter::getCount).isEqualTo(1L);
    }

    @Test
    void shouldMarkRequest() {
        // Given
        final HttpRequest postRequest = builder.patch("application/json", "{}").uri("/random").build();
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        kubernetesClientMetrics.before(builder, postRequest, emptyTags);
        final OperatorMetricUtils.SynchronizedMeterView requestRateMeter = kubernetesClientMetrics.getRequestRateMeter();

        // When
        requestRateMeter.update();

        // Then
        assertThat(requestRateMeter).extracting(OperatorMetricUtils.SynchronizedMeterView::getCount).isEqualTo(3L);
        // MeterView defaults to averaging over 60s, so we expect 3 / 60
        assertThat(requestRateMeter).extracting(OperatorMetricUtils.SynchronizedMeterView::getRate).asInstanceOf(DOUBLE)
                .isCloseTo(0.05, Offset.offset(0.0001));
    }

    @Test
    void shouldCountPostResponses() {
        // Given
        final HttpRequest postRequest = builder.post("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(postRequest, new StubHttpResponse(postRequest, Map.of(), 200), (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter()).extracting(Counter::getCount).isEqualTo(1L);
    }

    @Test
    void shouldCountDeleteResponse() {
        // Given
        final HttpRequest postRequest = builder.delete("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(postRequest, new StubHttpResponse(postRequest, Map.of(), 200), (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter()).extracting(Counter::getCount).isEqualTo(1L);
    }

    @Test
    void shouldCountPatchResponse() {
        // Given
        final HttpRequest postRequest = builder.patch("application/json", "{}").uri("/random").build();

        // When
        kubernetesClientMetrics.after(postRequest, new StubHttpResponse(postRequest, Map.of(), 200), (value, asyncBody) -> {});

        // Then
        assertThat(kubernetesClientMetrics.getResponseCounter()).extracting(Counter::getCount).isEqualTo(1L);
    }



    private static class StubHttpResponse implements HttpResponse<String> {
        private final HttpRequest request;
        private final Map<String, List<String>> headers;
        private final int statusCode;

        public StubHttpResponse(HttpRequest request, Map<String, List<String>> headers, int statusCode) {
            this.request = request;
            this.headers = headers;
            this.statusCode = statusCode;
        }

        @Override
        public int code() {
            return statusCode;
        }

        @Override
        public String body() {
            return "";
        }

        @Override
        public HttpRequest request() {
            return request;
        }

        @Override
        public Optional<HttpResponse<?>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public List<String> headers(String key) {
            return headers.get(key);
        }

        @Override
        public Map<String, List<String>> headers() {
            return headers;
        }
    }
}
