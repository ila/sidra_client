library(ggplot2)
library(dplyr)
library(ggpubr)
library(gridExtra)
library(ggplotify)
library(readr)
library(tidyr)
library(purrr)
library(stringr)
library(extrafont)
library(scales)
library(knitr)
library(cowplot)

# Define consistent color palette
color_palette <- c(
  "1" = "#72B01D",      # Green
  "2" = "#171738",      # Dark Blue
  "4" = "#eb34e1",      # Purple
  "6" = "#FF9F1C",      # Orange
  "1/window" = "#72B01D",  # Green (same as 1)
  "2/window" = "#171738",  # Dark Blue (same as 2)
  "4/window" = "#eb34e1",  # Purple (same as 4)
  "6/window" = "#FF9F1C"   # Orange (same as 6)
)

# Define consistent shape palette
shape_palette <- c(
  "1" = 16,      # Circle
  "2" = 17,      # Triangle
  "4" = 15,      # Square
  "6" = 18,      # Diamond
  "1/window" = 16,  # Circle (same as 1)
  "2/window" = 17,  # Triangle (same as 2)
  "4/window" = 15,  # Square (same as 4)
  "6/window" = 18   # Diamond (same as 6)
)

# todo - redo all the window experiments

# Define queries to exclude
queries_to_exclude <- c("7bbef8ec", "47387463", "cd1bb66d", "e49245cf", "5d2989b9")

# Read buffer size data
buffer_size <- read.csv("results/buffer_size.csv", row.names = NULL)
refresh_1 <- read.csv("results/refresh_1.csv", row.names = NULL)
refresh_2 <- read.csv("results/refresh_2.csv", row.names = NULL)
refresh_4 <- read.csv("results/refresh_4.csv", row.names = NULL)
refresh_6 <- read.csv("results/refresh_6.csv", row.names = NULL)

# Filter out excluded queries from refresh data
refresh_1 <- refresh_1 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
refresh_2 <- refresh_2 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
refresh_4 <- refresh_4 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
refresh_6 <- refresh_6 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

# Sum all the times for each refresh
refresh_1 <- refresh_1 %>%
  group_by(run) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")
refresh_2 <- refresh_2 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")
refresh_4 <- refresh_4 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")
refresh_6 <- refresh_6 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")

refresh_1$refresh_rate = "1/window"
refresh_2$refresh_rate = "2/window"
refresh_4$refresh_rate = "4/window"
refresh_6$refresh_rate = "6/window"

# Assign a column "hour" to each refresh data
refresh_1$hour <- 24
refresh_2$hour <- c(12, 24)
refresh_4$hour <- c(6, 12, 18, 24)
refresh_6$hour <- c(4, 8, 12, 16, 20, 24)
# Add the hour column
buffer_size$hour <- case_when(
  buffer_size$n_refreshes == 6 ~ 4 + (4 * buffer_size$refresh),      # 4,8,12,16,20,24
  buffer_size$n_refreshes == 4 ~ 6 + (6 * buffer_size$refresh),      # 6,12,18,24
  buffer_size$n_refreshes == 2 ~ 12 + (12 * buffer_size$refresh),    # 12,24
  buffer_size$n_refreshes == 1 ~ 24                                  # 24
)

# Combine all refresh data into a single data frame
refresh_data <- rbind(refresh_1, refresh_2, refresh_4, refresh_6)
# Split the data
refresh_others <- refresh_data %>% filter(refresh_rate != "1/window")
refresh_1_window <- refresh_data %>% filter(refresh_rate == "1/window")

buffer_size_others <- buffer_size %>% filter(n_refreshes != 1)
buffer_size_1_window <- buffer_size %>% filter(n_refreshes == 1)

time_plot <- ggplot() +
  # Plot other refresh rates first (background)
  geom_point(data = refresh_others, aes(x = hour, y = total_time, color = refresh_rate, shape = refresh_rate),
             size = ifelse(refresh_others$refresh_rate %in% c("2/window", "6/window"), 3, 2)) +
  geom_line(data = refresh_others, aes(x = hour, y = total_time, color = refresh_rate), linewidth = 1) +
  # Plot 1/window on top (foreground)
  geom_point(data = refresh_1_window, aes(x = hour, y = total_time, color = refresh_rate, shape = refresh_rate),
             size = ifelse(refresh_1_window$refresh_rate %in% c("2/window", "6/window"), 4, 3)) +
  scale_color_manual(values = color_palette) +
  scale_shape_manual(values = shape_palette) +
  scale_x_continuous(breaks = seq(4, 24, by = 2), limits = c(4, 24)) +
  scale_y_continuous(labels = scales::comma) +
  guides(
    color = guide_legend(ncol = 2, title.position = "left"),
    shape = "none"
  ) +
  labs(x = "Hour / 24h Window", y = "Refresh Time (ms)", color = "Refresh Rate") +
    theme_minimal(base_size = 14, base_family = "Linux Libertine") +
    theme(
      #legend.margin = margin(c(0, 0, 0, -50)),
      axis.text.x = element_text(size = 12),
      axis.text.y = element_text(size = 15),
      axis.title.y = element_text(size = 12),

    )

buffer_plot <- ggplot() +
  # Plot other refresh rates first (background)
  geom_point(data = buffer_size_others, aes(x = hour, y = avg_buffer_size, color = factor(n_refreshes), shape = factor(n_refreshes)),
             size = ifelse(buffer_size_others$n_refreshes %in% c(2, 6), 3, 2)) +
  geom_line(data = buffer_size_others, aes(x = hour, y = avg_buffer_size, color = factor(n_refreshes)), linewidth = 1) +
  # Plot 1 refresh on top (foreground)
  geom_point(data = buffer_size_1_window, aes(x = hour, y = avg_buffer_size, color = factor(n_refreshes), shape = factor(n_refreshes)),
             size = ifelse(buffer_size_1_window$n_refreshes %in% c(2, 6), 4, 3)) +
  scale_color_manual(values = color_palette) +
  scale_shape_manual(values = shape_palette) +
  scale_x_continuous(breaks = seq(4, 24, by = 2), limits = c(4, 24)) +
  labs(x = "Hour / 24h Window", y = "Staging Area Size (%)", color = "Number of Refreshes") +
  guides(
    color = guide_legend(ncol = 2, title.position = "left"),
    shape = "none"
  ) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 12),
    axis.text.y = element_text(size = 15),
    axis.title.y = element_text(size = 12)
  )

# Extract legend from one of the plots with common legend
get_legend <- function(myggplot){
  tmp <- ggplot_gtable(ggplot_build(myggplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}

# Get the common legend
# Adjust left margin of the legend
#plot_5 <- plot_5 + theme(legend.margin = margin(c(0, 0, 0, 40)))
common_legend <- get_legend(time_plot)

buffer_plot <- buffer_plot + theme(legend.title = element_blank(), legend.position = "none")
time_plot <- time_plot + theme(legend.position = "none")

png("plots/refresh_combined_plot.png", width = 2000, height = 800, res = 350)
grid.arrange(
  arrangeGrob(time_plot, buffer_plot,
              ncol = 2, nrow = 1),
  common_legend,
  heights = c(10, 2)
)
dev.off()

# Now the completeness
completeness <- read.csv("results/completeness.csv", row.names = NULL, sep = ",")
window_1 <- read.csv("results/window_1.csv", row.names = NULL)
window_2 <- read.csv("results/window_2.csv", row.names = NULL)
window_4 <- read.csv("results/window_4.csv", row.names = NULL)
window_6 <- read.csv("results/window_6.csv", row.names = NULL)

# Filter out excluded queries from window data
window_1 <- window_1 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
window_2 <- window_2 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
window_4 <- window_4 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)
window_6 <- window_6 %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

# Sum all the times for each window
window_1 <- window_1 %>%
  group_by(run) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")
window_2 <- window_2 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")
window_4 <- window_4 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")
window_6 <- window_6 %>%
    group_by(run) %>%
    summarise(total_time = sum(time_ms), .groups = "drop")

window_1$window_size = "1"
window_2$window_size = "2"
window_4$window_size = "4"
window_6$window_size = "6"

# Combine all window data into a single data frame
window_data <- rbind(window_1, window_2, window_4, window_6)

time_plot_2 <- ggplot() +
  # Plot other window sizes first (background)
  geom_point(data = window_data, aes(x = run, y = total_time, color = factor(window_size), shape = factor(window_size), group = window_size),
             size = ifelse(window_data$window_size %in% c("2", "6"), 4, 3)) +
  geom_line(data = window_data, aes(x = run, y = total_time, color = factor(window_size), group = window_size), linewidth = 1.1) +
  scale_color_manual(values = color_palette) +
  scale_shape_manual(values = shape_palette) +
  guides(
    color = guide_legend(ncol = 2, title.position = "left"),
    shape = "none"
  ) +
  scale_y_continuous(labels = scales::comma) +
  labs(x = "Refresh", y = "Refresh Time (ms)", color = "Window Size") +
    theme_minimal(base_size = 14, base_family = "Linux Libertine") +
    theme(
      axis.text.x = element_text(size = 15),
      axis.text.y = element_text(size = 15),
      axis.title.y = element_text(size = 13),
    )

completeness_plot <- ggplot() +
  geom_point(data = completeness, aes(x = refresh_n, y = avg_completeness, color = factor(refreshes_day), shape = factor(refreshes_day)),
             size = ifelse(completeness$refreshes_day %in% c(2, 6), 3, 2)) +
  geom_line(data = completeness, aes(x = refresh_n, y = avg_completeness, color = factor(refreshes_day), group = refreshes_day, linetype = "Completeness"), linewidth = 1) +
  geom_point(data = completeness, aes(x = refresh_n, y = avg_buffer_size, color = factor(refreshes_day), shape = factor(refreshes_day)),
             size = ifelse(completeness$refreshes_day %in% c(2, 6), 3, 2)) +
  geom_line(data = completeness, aes(x = refresh_n, y = avg_buffer_size, color = factor(refreshes_day), group = refreshes_day, linetype = "Buffer Size"), linewidth = 1) +
  scale_color_manual(values = color_palette) +
  scale_shape_manual(values = shape_palette) +
  scale_linetype_manual(values = c("Completeness" = "solid", "Buffer Size" = "dotted")) +
  labs(x = "Refresh", y = "Completeness (%)", color = "Windows/Day  ", linetype = "Metric") +
  guides(
    color = guide_legend(ncol = 2, title.position = "left"),
    shape = "none",
    linetype = guide_legend(title.position = "left")
  ) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.margin = margin(c(0, 0, 0, 0)),
    legend.box = "horizontal",
    axis.text.x = element_text(size = 15),
    axis.text.y = element_text(size = 15),
    axis.title.y = element_text(size = 13)
  )

common_legend_2 <- get_legend(completeness_plot)

time_plot_2 <- time_plot_2 + theme(legend.title = element_blank(), legend.position = "none")
completeness_plot <- completeness_plot + theme(legend.position = "none")

png("plots/completeness_combined_plot.png", width = 2000, height = 850, res = 350)
grid.arrange(
  arrangeGrob(time_plot_2, completeness_plot,
              ncol = 2, nrow = 1),
  common_legend_2,
  heights = c(10, 2)
)
dev.off()