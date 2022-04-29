package dev.vality.schedulator.service;

import dev.vality.damsel.schedule.ContextValidationResponse;
import dev.vality.damsel.schedule.ScheduledJobExecutorSrv;
import dev.vality.woody.api.flow.error.WUnavailableResultException;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.ByteBuffer;

@Slf4j
@Service
public class RemoteClientManager {

    public ContextValidationResponse validateExecutionContext(String url, ByteBuffer context) throws TException {
        try {
            ScheduledJobExecutorSrv.Iface client = getRemoteClient(url);
            return client.validateExecutionContext(context);
        } catch (Exception e) {
            log.error("Call remote client failed", e);
            throw new WUnavailableResultException(e);
        }
    }

    public ScheduledJobExecutorSrv.Iface getRemoteClient(String url) {
        return new THSpawnClientBuilder()
                .withAddress(URI.create(url))
                .build(ScheduledJobExecutorSrv.Iface.class);
    }

}
