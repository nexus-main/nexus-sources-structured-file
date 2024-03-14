using System.Globalization;

internal record struct CustomDateTimeOffset
{
    public CustomDateTimeOffset(DateTime dateTime, TimeSpan offset)
    {
        if (dateTime.Kind != DateTimeKind.Unspecified)
            throw new Exception("Only Kind = DateTimeKind.Unspecified is allowed.");

        DateTime = dateTime;
        Offset = offset;

        UtcDateTime = DateTime == default && Offset > TimeSpan.Zero
            ? new DateTime(0, DateTimeKind.Utc)
            : new DateTimeOffset(DateTime, Offset).UtcDateTime;
    }

    public DateTime DateTime { get; }

    public DateTime UtcDateTime { get; }
    
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
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal | DateTimeStyles.NoCurrentDateDefault,
            out var tmpDateTime
        );

        var result2 = DateTimeOffset.TryParseExact(
            input,
            format,
            default,
            DateTimeStyles.AssumeUniversal,
            out var tmpDateTimeOffset
        );

        if (!result1 || !result2)
            return false;

        /* This happens when there is no date in the input string 
         * (see also 'NoCurrentDateDefault', which does not work
         * for DateTimeOffset) 
         */
        if (tmpDateTime.Date != tmpDateTimeOffset.UtcDateTime.Date)
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

        /* timezone information in file path */
        if (
#warning improve
            input.Contains("Z") ||
            input.Contains("+"))
        {
            // do nothing
        }

        /* no timezone information in file path */
        else
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