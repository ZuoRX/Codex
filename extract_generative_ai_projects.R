#!/usr/bin/env Rscript

# 提取社科基金中与“生成式人工智能”相关的项目
# 用法：
# Rscript extract_generative_ai_projects.R \
#   --input "0 2023-2025社科立项基金.xlsx" \
#   --output "生成式人工智能_社科基金筛选结果.xlsx"

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(purrr)
  library(stringr)
  library(tidyr)
  library(writexl)
})

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  idx <- which(args == flag)
  if (length(idx) == 0) return(default)
  if (idx == length(args)) stop(sprintf("参数 %s 缺少取值", flag))
  args[idx + 1]
}

input_file <- get_arg("--input", "0 2023-2025社科立项基金.xlsx")
output_file <- get_arg("--output", "生成式人工智能_社科基金筛选结果.xlsx")

if (!file.exists(input_file)) {
  stop(sprintf("输入文件不存在: %s", input_file))
}

# 可根据需要增删关键词
keywords <- c(
  "生成式人工智能", "生成式AI", "AIGC", "大模型", "预训练模型",
  "语言模型", "多模态", "文生图", "文生视频", "智能生成"
)

# 组合为一个正则，忽略大小写
keyword_pattern <- regex(str_c(keywords, collapse = "|"), ignore_case = TRUE)

sheet_names <- excel_sheets(input_file)

message("读取工作表数量: ", length(sheet_names))

result <- map_dfr(sheet_names, function(sheet_name) {
  df <- read_excel(input_file, sheet = sheet_name, guess_max = 5000)

  if (nrow(df) == 0) {
    return(tibble())
  }

  df2 <- df %>%
    mutate(across(everything(), as.character)) %>%
    mutate(.sheet = sheet_name,
           .row_id = row_number())

  # 把每行所有文本列拼起来做匹配（避免依赖固定字段名）
  searchable_cols <- names(df2)

  matched <- df2 %>%
    unite(".full_text", all_of(searchable_cols), sep = " | ", remove = FALSE, na.rm = TRUE) %>%
    mutate(
      .hit = str_detect(.full_text, keyword_pattern),
      .matched_keywords = map_chr(
        .full_text,
        ~ str_c(keywords[str_detect(.x, regex(keywords, ignore_case = TRUE))], collapse = "；")
      )
    ) %>%
    filter(.hit) %>%
    select(.sheet, .row_id, .matched_keywords, everything(), -.full_text, -.hit)

  matched
})

if (nrow(result) == 0) {
  message("未检索到包含关键词的项目。")
} else {
  message("命中条目数: ", nrow(result))
}

write_xlsx(list("生成式人工智能相关项目" = result), path = output_file)

# 同时输出 CSV 便于快速查看
csv_file <- sub("\\.xlsx$", ".csv", output_file, ignore.case = TRUE)
readr::write_excel_csv(result, csv_file)

message("已输出: ", output_file)
message("已输出: ", csv_file)
