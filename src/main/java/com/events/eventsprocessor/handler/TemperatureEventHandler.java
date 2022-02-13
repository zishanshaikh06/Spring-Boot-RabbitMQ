package com.events.eventsprocessor.handler;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esperio.amqp.AMQPSink;
import com.events.eventsprocessor.event.TemperatureEvent;
import com.events.eventsprocessor.subscriber.StatementSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This class handles incoming Temperature Events. It processes them through the EPService, to which
 * it has attached the 3 queries.
 */
@Component
@Scope(value = "singleton")
public class TemperatureEventHandler implements InitializingBean {

    /**
     * Logger
     */
    private static Logger LOG = LoggerFactory.getLogger(TemperatureEventHandler.class);

    /**
     * Esper service
     */
    private EPServiceProvider epService;
    private EPStatement criticalEventStatement;
    private EPStatement warningEventStatement;
    private EPStatement monitorEventStatement;

    @Autowired
    @Qualifier("criticalEventSubscriber")
    private StatementSubscriber criticalEventSubscriber;

    @Autowired
    @Qualifier("warningEventSubscriber")
    private StatementSubscriber warningEventSubscriber;

    @Autowired
    @Qualifier("monitorEventSubscriber")
    private StatementSubscriber monitorEventSubscriber;

    /**
     * Configure Esper Statement(s).
     */
    public void initService() {
        LOG.info("Initializing Servcie ..");
        Configuration config = new Configuration();
        config.addEventTypeAutoName("com.events.eventsprocessor");
        config.addImport(AMQPSink.class.getPackage().getName()+".*");
        config.addEventType("OutputEvent", TemperatureEvent.class);
        config.addEventType(TemperatureEvent.class);
        epService = EPServiceProviderManager.getDefaultProvider(config);
        /*
        EPRuntime cepRT = epService.getEPRuntime();
        EPAdministrator cepAdm = epService.getEPAdministrator();
        cepAdm.getConfiguration().addImport(AMQPSink.class.getPackage().getName() + ".*");

        String epl2 = "create Dataflow AMQPOutgoingDataFlow \n"
                + "EventBusSource -> outstream<OutputEvent>{} \n"
                + "AMQPSink(outstream)"
                + "{host: 'localhost',"
                + "port: 5672,"
                + "username:'guest',"
                + "password:'guest',"
                + "exchange: 'temperature.exchange',"
                + "routingKey: 'temperature.routingkey',"
                + "queueName: 'temperature.queue',"
                + "declareDurable: false,"
                + "declareExclusive: false,"
                + "declareAutoDelete: false,"
                + "waitMSecNextMsg: 0,"
                + "collector: {class: 'ObjectToAMQPCollectorSerializable'}, logMessages: true }";
        cepAdm.createEPL(epl2);
        EPDataFlowInstance instance2 = cepRT.getDataFlowRuntime().instantiate(
                "AMQPOutgoingDataFlow");
        instance2.start();
         */

        createCriticalTemperatureCheckExpression();
        createWarningTemperatureCheckExpression();
        createTemperatureMonitorExpression();
    }

    /**
     * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
     * than the first event. This is checking for a sudden, sustained escalating rise in the
     * temperature
     */
    private void createCriticalTemperatureCheckExpression() {
        LOG.info("create Critical Temperature Check Expression");
        criticalEventStatement = epService.getEPAdministrator().createEPL(criticalEventSubscriber.getStatement());
        criticalEventStatement.setSubscriber(criticalEventSubscriber);
    }

    /**
     * EPL to check for 2 consecutive Temperature events over the threshold - if matched, will alert
     * listener.
     */
    private void createWarningTemperatureCheckExpression() {
        LOG.info("create Warning Temperature Check Expression");
        warningEventStatement = epService.getEPAdministrator().createEPL(warningEventSubscriber.getStatement());
        warningEventStatement.setSubscriber(warningEventSubscriber);
    }

    /**
     * EPL to monitor the average temperature every 10 seconds. Will call listener on every event.
     */
    private void createTemperatureMonitorExpression() {
        LOG.info("create Timed Average Monitor");
        monitorEventStatement = epService.getEPAdministrator().createEPL(monitorEventSubscriber.getStatement());
        monitorEventStatement.setSubscriber(monitorEventSubscriber);
    }

    /**
     * Handle the incoming TemperatureEvent.
     */
    public void handle(TemperatureEvent event) {
        LOG.info(event.toString());
        epService.getEPRuntime().sendEvent(event);
    }

    @Override
    public void afterPropertiesSet() {
        LOG.info("Configuring..");
        initService();
    }
}
