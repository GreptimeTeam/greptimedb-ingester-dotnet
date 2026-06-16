namespace GreptimeDB.Ingester.Client;

internal static class GreptimeStatusCodes
{
    /// <summary>
    /// gRPC trailer key carrying GreptimeDB's business status code on an errored
    /// response (mirrors common_error::GREPTIME_DB_HEADER_ERROR_CODE). The
    /// server's status-code to gRPC-code mapping is lossy (e.g. RegionBusy and
    /// RateLimited both surface as ResourceExhausted), so this precise code is
    /// what distinguishes retryable conditions from terminal ones.
    /// </summary>
    public const string ErrorCodeTrailer = "x-greptime-err-code";

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
