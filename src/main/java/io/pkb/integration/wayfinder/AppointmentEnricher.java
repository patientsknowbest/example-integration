package io.pkb.integration.wayfinder;

import ca.uhn.fhir.rest.client.api.IGenericClient;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.hl7.fhir.r4.model.Appointment;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Location;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Reference;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * For Appointment resources, 
 */
public class AppointmentEnricher implements ResourceEnricher<Appointment> {
    private static final Logger log = Logger.getLogger(MethodHandles.lookup().lookupClass().getName());
    private final IGenericClient fhirClient;
    // We're happy with this b/c orgs don't change much.
    private final Cache<String, Organization> orgCache;

    public AppointmentEnricher(IGenericClient fhirClient) {
        this.fhirClient = fhirClient;
        this.orgCache = CacheBuilder.newBuilder()
                .build();
    }
    
    @Override
    public void enrich(Appointment appointment) {
        Optional.ofNullable(appointment.getMeta().getExtensionByUrl("http://fhir.patientsknowbest.com/structuredefinition/source-organisation"))
                        .map(sourceOrgExt -> ((Reference) sourceOrgExt.getValue()).getReferenceElement().getIdPart())
                        .map(sourceOrgId -> {
                            Organization sourceOrg;
                            try {
                                sourceOrg = this.orgCache.get(sourceOrgId, () ->
                                        fhirClient.read()
                                                .resource(Organization.class)
                                                .withId(sourceOrgId)
                                                .execute()
                                );
                            } catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                            return sourceOrg;
                        })
                        .flatMap(org -> org.getIdentifier().stream().filter(orgIdentifier -> Objects.equals(orgIdentifier.getSystem(), "https://fhir.nhs.uk/Id/ods-organization-code"))
                                .findFirst())
                        .ifPresent(ODScodeIdentifier -> {
                            log.log(Level.INFO, "adding source ORG ODS {} to appointment {}", new Object[]{ODScodeIdentifier.getValue(), appointment.getId()});
                            var ref = new Reference();
                            ref.setType(new Location().fhirType());
                            ref.setIdentifier(ODScodeIdentifier.copy());
                            var participant = new Appointment.AppointmentParticipantComponent();
                            participant.setActor(ref);
                            appointment.addParticipant(participant);
                        });
       
        
    }

    @Override
    public Class<Appointment> type() {
        return Appointment.class;
    }
}
