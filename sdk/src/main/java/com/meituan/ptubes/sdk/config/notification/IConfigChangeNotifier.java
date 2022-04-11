package com.meituan.ptubes.sdk.config.notification;

/**
 * lifestyle by ptubes: getConfig --> registerAllListener -->  deRegisterAllListener --> shutdown
 * you can also invoke this function to registering or deregistering anytime.
 *
 * @description
 */
public interface IConfigChangeNotifier {
    <T> T getConfig(String confName, Class<T> confClass);

    void registerAllListener();

    void deRegisterAllListener();

    void setConfChangedRecipient(DefaultConfChangedRecipient defaultConfChangedRecipient);

    void shutdown();
}
