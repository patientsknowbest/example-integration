package io.pkb.integration.wayfinder;

import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Business rules for enhancing each type of resource are implemented in subclasses.
 * @param <T>
 */
public interface ResourceEnricher<T extends IBaseResource> {
    void enrich(T t);
    Class<T> type();
}
