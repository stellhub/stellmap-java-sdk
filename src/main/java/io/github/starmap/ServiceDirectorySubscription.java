package io.github.starmap;

/** 带本地目录缓存的 watch 订阅句柄。 */
public interface ServiceDirectorySubscription extends RegistryWatchSubscription {

    /**
     * 返回本地目录视图。
     *
     * @return 本地目录
     */
    ServiceDirectory getServiceDirectory();

    /**
     * 返回当前订阅记录的最后 revision。
     *
     * @return 最后处理的 revision
     */
    long getLastRevision();
}
