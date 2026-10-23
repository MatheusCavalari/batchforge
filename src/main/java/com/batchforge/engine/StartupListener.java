package com.batchforge.engine;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class StartupListener {

    @Inject BatchForgeEngine engine;

    void onStart(@Observes StartupEvent event) {
        engine.start();
    }
}
