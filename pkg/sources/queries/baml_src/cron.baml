class CronResult {
  cron_expr string @description("Standard 5-field cron expression: minute hour day_of_month month day_of_week")
}

function ParseCronSchedule(description: string) -> CronResult {
  client SmallModelCerebras
  prompt #"
    Convert the following human-readable schedule description into a standard
    5-field cron expression (minute hour day_of_month month day_of_week).

    Description: {{ description }}

    {{ ctx.output_format }}
  "#
}
