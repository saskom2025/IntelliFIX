package org.intellifix.fix;

import lombok.extern.slf4j.Slf4j;
import org.intellifix.fix.apps.HubApp;
import org.intellifix.redis.base.MessagePublisher;
import org.intellifix.redis.RedisMessagePublisher;
import quickfix.*;

@Slf4j
public class HubSimulator {

    private final static String CLIENT_COMP_ID = "ClientCompID";
    private final static String BROKER_COMP_ID = "BrokerCompID";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Missing configuration file path");
            System.exit(1);
        }

        MessagePublisher messagePublisher = new RedisMessagePublisher();

        String configPath = args[0];
        SessionSettings settings = new SessionSettings(configPath);

        String clientCompID = settings.getString(CLIENT_COMP_ID);
        String brokerCompID = settings.getString(BROKER_COMP_ID);

        HubApp app = new HubApp(messagePublisher, clientCompID, brokerCompID);

        MessageStoreFactory storeFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();

        Acceptor acceptor = new SocketAcceptor(app, storeFactory, settings, logFactory, messageFactory);
        Initiator initiator = new SocketInitiator(app, storeFactory, settings, logFactory, messageFactory);

        log.info("Starting Hub...");
        acceptor.start();
        initiator.start();

        log.info("Hub is running. Acceptor on Client port, Initiator on Broker port.");

        // Keeps running until interrupted
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping Hub...");
            acceptor.stop();
            initiator.stop();
        }));

        while (true) {
            Thread.sleep(1000);
        }
    }
}
