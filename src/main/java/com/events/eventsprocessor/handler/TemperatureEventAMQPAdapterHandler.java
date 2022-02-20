package com.events.eventsprocessor.handler;

import com.espertech.esper.client.*;
import com.espertech.esper.client.dataflow.EPDataFlowInstance;
import com.espertech.esperio.amqp.AMQPSink;
import com.espertech.esperio.amqp.EsperAMQPSerializer;
import com.events.eventsprocessor.event.TemperatureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This class handles incoming Temperature Events. It processes them through the EPService, to which
 * it has attached the 3 queries.
 */
@Component
@Scope(value = "singleton")
public class TemperatureEventAMQPAdapterHandler implements InitializingBean {

    private static Logger LOG = LoggerFactory.getLogger(TemperatureEventAMQPAdapterHandler.class);

    private EPServiceProvider epService;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.port}")
    private String port;

    @Value("${spring.rabbitmq.temperature.exchange}")
    private String temperatureExchange;

    @Value("${spring.rabbitmq.temperature.routingkey}")
    private String temperatureRoutingkey;

    @Value("${spring.rabbitmq.temperature.queue}")
    private String temperatureQueue;

    /**
     * Configure Esper Statement(s).
     */
    public void initService() {
        LOG.info("Initializing Service ..");
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);
        EPRuntime epRuntime = epService.getEPRuntime();
        EPAdministrator epAdministrator = epService.getEPAdministrator();
        epAdministrator.getConfiguration().addImport(AMQPSink.class.getPackage().getName() + ".*");
        epAdministrator.getConfiguration().addImport(EsperAMQPSerializer.class.getPackage().getName() + ".*");
        epAdministrator.getConfiguration().addImport(TemperatureEvent.class.getPackage().getName() + ".*");

        createTemperatureEventAMQPAdapter(epRuntime, epAdministrator);
    }

    private void createTemperatureEventAMQPAdapter(EPRuntime epRuntime, EPAdministrator epAdministrator){
        String epl2 = "create Dataflow AMQPOutgoingDataFlow \n"
                + "EventBusSource -> outstream<TemperatureEvent>{} \n"
                + "AMQPSink(outstream)"
                + "{host: '"+host+"',"
                + "port: "+port+","
                + "username:'"+username+"',"
                + "password:'"+password+"',"
                + "exchange: '"+temperatureExchange+"',"
                + "routingKey: '"+temperatureRoutingkey+"',"
                + "declareDurable: true,"
                + "declareExclusive: false,"
                + "declareAutoDelete: false,"
                + "waitMSecNextMsg: 0,"
                + "collector: {class: 'EsperAMQPSerializer'}, logMessages: true }";
        epAdministrator.createEPL(epl2);
        EPDataFlowInstance dataFlowInstance = epRuntime.getDataFlowRuntime().instantiate(
                "AMQPOutgoingDataFlow");
        dataFlowInstance.start();
    }

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
