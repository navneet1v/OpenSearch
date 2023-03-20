/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchPhase;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchPhaseResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.TAG_KEY;
import static org.opensearch.ingest.Pipeline.DESCRIPTION_KEY;
import static org.opensearch.ingest.Pipeline.VERSION_KEY;

/**
 * Concrete representation of a search pipeline, holding multiple processors.
 */
class Pipeline {

    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    public static final String PHASE_PROCESSORS_KEY = "phase_processors";
    private final String id;
    private final String description;
    private final Integer version;

    // TODO: Refactor org.opensearch.ingest.CompoundProcessor to implement our generic Processor interface
    // Then these can be CompoundProcessors instead of lists.
    private final List<SearchRequestProcessor> searchRequestProcessors;
    private final List<SearchResponseProcessor> searchResponseProcessors;

    private final List<SearchPhaseProcessor> searchPhaseProcessors;

    Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        List<SearchPhaseProcessor> phaseProcessorConfigs
    ) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.searchRequestProcessors = requestProcessors;
        this.searchResponseProcessors = responseProcessors;
        this.searchPhaseProcessors = phaseProcessorConfigs;
    }

    public static Pipeline create(String id, Map<String, Object> config, Map<String, Processor.Factory> processorFactories)
        throws Exception {
        String description = ConfigurationUtils.readOptionalStringProperty(null, null, config, DESCRIPTION_KEY);
        Integer version = ConfigurationUtils.readIntProperty(null, null, config, VERSION_KEY, null);
        List<Map<String, Object>> requestProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, REQUEST_PROCESSORS_KEY);
        List<SearchRequestProcessor> requestProcessors = readProcessors(
            SearchRequestProcessor.class,
            processorFactories,
            requestProcessorConfigs
        );
        List<Map<String, Object>> responseProcessorConfigs = ConfigurationUtils.readOptionalList(
            null,
            null,
            config,
            RESPONSE_PROCESSORS_KEY
        );

        List<Map<String, Object>> phaseProcessorConfigs = ConfigurationUtils.readOptionalList(null, null, config, PHASE_PROCESSORS_KEY);
        List<SearchResponseProcessor> responseProcessors = readProcessors(
            SearchResponseProcessor.class,
            processorFactories,
            responseProcessorConfigs
        );
        final List<SearchPhaseProcessor> phaseProcessors = readProcessors(
            SearchPhaseProcessor.class,
            processorFactories,
            phaseProcessorConfigs
        );
        if (config.isEmpty() == false) {
            throw new OpenSearchParseException(
                "pipeline ["
                    + id
                    + "] doesn't support one or more provided configuration parameters "
                    + Arrays.toString(config.keySet().toArray())
            );
        }
        return new Pipeline(id, description, version, requestProcessors, responseProcessors, phaseProcessors);
    }

    @SuppressWarnings("unchecked") // Cast is checked using isInstance
    private static <T extends Processor> List<T> readProcessors(
        Class<T> processorType,
        Map<String, Processor.Factory> processorFactories,
        List<Map<String, Object>> requestProcessorConfigs
    ) throws Exception {
        List<T> processors = new ArrayList<>();
        if (requestProcessorConfigs == null) {
            return processors;
        }
        for (Map<String, Object> processorConfigWithKey : requestProcessorConfigs) {
            for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                String type = entry.getKey();
                if (!processorFactories.containsKey(type)) {
                    throw new IllegalArgumentException("Invalid processor type " + type);
                }
                Map<String, Object> config = (Map<String, Object>) entry.getValue();
                String tag = ConfigurationUtils.readOptionalStringProperty(null, null, config, TAG_KEY);
                String description = ConfigurationUtils.readOptionalStringProperty(null, tag, config, DESCRIPTION_KEY);
                Processor processor = processorFactories.get(type).create(processorFactories, tag, description, config);
                if (processorType.isInstance(processor)) {
                    processors.add((T) processor);
                } else {
                    throw new IllegalArgumentException("Processor type " + type + " is not a " + processorType.getSimpleName());
                }
            }
        }
        return processors;
    }

    List<Processor> flattenAllProcessors() {
        List<Processor> allProcessors = new ArrayList<>(searchRequestProcessors.size() + searchResponseProcessors.size());
        allProcessors.addAll(searchRequestProcessors);
        allProcessors.addAll(searchResponseProcessors);
        return allProcessors;
    }

    String getId() {
        return id;
    }

    String getDescription() {
        return description;
    }

    Integer getVersion() {
        return version;
    }

    List<SearchRequestProcessor> getSearchRequestProcessors() {
        return searchRequestProcessors;
    }

    List<SearchResponseProcessor> getSearchResponseProcessors() {
        return searchResponseProcessors;
    }

    SearchRequest transformRequest(SearchRequest originalRequest) throws SearchPipelineProcessingException {
        if (CollectionUtils.isEmpty(searchRequestProcessors) == false) {
            try {
                // Save the original request by deep cloning the existing request.
                BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
                originalRequest.writeTo(bytesStreamOutput);
                SearchRequest request = new SearchRequest(bytesStreamOutput.bytes().streamInput());
                for (SearchRequestProcessor searchRequestProcessor : searchRequestProcessors) {
                    request = searchRequestProcessor.processRequest(originalRequest);
                }
                return request;
            } catch (Exception e) {
                throw new SearchPipelineProcessingException(e);
            }
        }
        return originalRequest;
    }

    SearchResponse transformResponse(SearchRequest request, SearchResponse response) throws SearchPipelineProcessingException {
        try {
            for (SearchResponseProcessor responseProcessor : searchResponseProcessors) {
                response = responseProcessor.processResponse(request, response);
            }
            return response;
        } catch (Exception e) {
            throw new SearchPipelineProcessingException(e);
        }
    }

    <Result extends SearchPhaseResult> SearchPhaseResults<Result> runSearchPhaseTransformer(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext context, SearchPhase currentPhase, SearchPhase nextPhase
    ) throws SearchPipelineProcessingException {
        try {
            for (SearchPhaseProcessor searchPhaseProcessor : searchPhaseProcessors) {
                if(currentPhase.getName().equals(searchPhaseProcessor.getBeforePhase().getName()) && nextPhase.getName().equals(searchPhaseProcessor.getAfterPhase().getName())) {
                    searchPhaseResult = searchPhaseProcessor.execute(searchPhaseResult, context);
                }
            }
            return searchPhaseResult;
        } catch (Exception e) {
            throw new SearchPipelineProcessingException(e);
        }
    }
}
