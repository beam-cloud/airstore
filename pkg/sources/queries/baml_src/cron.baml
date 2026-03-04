class CronResult {
  cron_expr string @description("Standard 5-field cron expression: minute hour day_of_month month day_of_week")
}

function ParseCronSchedule(description: string, timezone: string) -> CronResult {
  client SmallModelCerebras
  prompt #"
    Convert the following human-readable schedule description into a standard
    5-field cron expression (minute hour day_of_month month day_of_week).

    The user is in the {{ timezone }} timezone. All times in the description
    refer to {{ timezone }} local time. Output the cron expression in that
    local timezone (do NOT convert to UTC).

    Description: {{ description }}

    {{ ctx.output_format }}
  "#
}
