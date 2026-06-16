namespace GreptimeDB.Ingester.Client;

internal static class GreptimeStatusCodes
{
    public const uint Success = 0;
    public const uint Internal = 1003;
    public const uint InvalidArguments = 1004;
    public const uint DeadlineExceeded = 1008;
    public const uint RegionNotReady = 4008;
    public const uint RegionBusy = 4009;
    public const uint TableUnavailable = 4010;
    public const uint StorageUnavailable = 5000;
    public const uint RuntimeResourcesExhausted = 6000;
    public const uint RateLimited = 6001;
}
