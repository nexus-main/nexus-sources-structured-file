using System.Globalization;

internal record struct CustomDateTimeOffset
{
    private DateTime? _utcDateTime;

    public CustomDateTimeOffset(DateTime dateTime, TimeSpan offset)
    {
        if (dateTime.Kind != DateTimeKind.Unspecified)
            throw new Exception("Only Kind = DateTimeKind.Unspecified is allowed.");

        DateTime = dateTime;
        Offset = offset;
    }

    public DateTime DateTime { get; }

    public DateTime UtcDateTime
    {
        get
        {
            if (!_utcDateTime.HasValue)
            {
                _utcDateTime = (DateTime - DateTime.MinValue) <= Offset
                    ? new DateTime(0, DateTimeKind.Utc)
                    : new DateTimeOffset(DateTime, Offset).UtcDateTime;
            }

            return _utcDateTime.Value;
        }
    } 

    public TimeSpan Offset { get; }

    public static bool TryParseExact(
        string input,
        string format,
        TimeSpan utcOffset,
        out CustomDateTimeOffset dateTimeOffset)
    {
        dateTimeOffset = default;

        var result1 = DateTime.TryParseExact(
            input,
            format,
            default,
            /* Detect if input string includes time-zone information */
            DateTimeStyles.AdjustToUniversal |
            /* Do not use today as date when input contains no date information */
            DateTimeStyles.NoCurrentDateDefault,
            out var tmpDateTime
        );

        var result2 = DateTimeOffset.TryParseExact(
            input,
            format,
            default,
            /* Ensure that the Offset is 00:00 and so `tmpDateTimeOffset.UtcDateTime` 
             * becomes comparable to `tmpDateTime.Date` which is being adjusted to
             * universal.
             */
            DateTimeStyles.AssumeUniversal,
            out var tmpDateTimeOffset
        );

        if (!result1 || !result2)
            return false;

        /* This happens when there is no date in the input string 
         * (see also 'NoCurrentDateDefault', which does not work
         * for DateTimeOffset) 
         */
        if (
            /* AdjustToUniversal = UTC */
            tmpDateTime.Date !=
            /* UtcDateTime = UTC */
            tmpDateTimeOffset.UtcDateTime.Date
        )
        {
            dateTimeOffset = new CustomDateTimeOffset(
                new DateTime(01, 01, 0001) + tmpDateTimeOffset.TimeOfDay,
                tmpDateTimeOffset.Offset
            );
        }

        else
        {
            dateTimeOffset = FromDateTimeOffset(tmpDateTimeOffset);
        }

        /* No timezone information found in input */
        if (tmpDateTime.Kind == DateTimeKind.Unspecified)
        {
            dateTimeOffset = new CustomDateTimeOffset
            (
                dateTimeOffset.DateTime,
                utcOffset
            );
        }

        return true;
    }

    private static CustomDateTimeOffset FromDateTimeOffset(DateTimeOffset value)
    {
        return new CustomDateTimeOffset(value.DateTime, value.Offset);
    }
}