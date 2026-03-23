# === Package Installer ===
packages <- c("ggplot2", "dplyr", "ggpubr", "gridExtra", "ggplotify", "readr", "tidyr", "purrr", "stringr", "extrafont", "scales", "knitr", "cowplot")
# Set CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!(pkg %in% installed)) {
    install.packages(pkg, dependencies = TRUE)
  }
}

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

# font_import()

# Example centralized and decentralized data
# Replace this with your actual data frames
centralized_10 <- read.csv("results/results_centralized_10.csv")
centralized_100 <- read.csv("results/results_centralized_100.csv")
centralized_1000 <- read.csv("results/results_centralized_1000.csv")
decentralized <- read.csv("results/results_decentralized.csv")
decentralized_small <- read.csv("results/results_decentralized_small.csv")

# cpu data
cpu_decentralized <- read.csv("results/cpu_decentralized.csv")
cpu_centralized_10 <- read.csv("results/cpu_centralized_10.csv")
cpu_centralized_1000 <- read.csv("results/cpu_centralized_1000.csv")
cpu_centralized_100 <- read.csv("results/cpu_centralized_100.csv")
cpu_decentralized_small <- read.csv("results/cpu_decentralized_small.csv")

# Add source labels
centralized_10$system <- "10 data points/window (xxl)"
centralized_100$system <- "100 data points/window (xxl)"
centralized_1000$system <- "1000 data points/window (xxl)"
decentralized$system <- "Decentralized, 1 data point/window (xxl)"
decentralized_small$system <- "Decentralized, 1 data point/window (l)"

# cpu data
cpu_decentralized$system <- "Decentralized, 1 data point/window (xxl)"
cpu_centralized_10$system <- "10 data points/window (xxl)"
cpu_centralized_1000$system <- "1000 data points/window (xxl)"
cpu_centralized_100$system <- "100 data points/window (xxl)"
cpu_decentralized_small$system <- "Decentralized, 1 data point/window (l)"

queries_to_exclude <- c("7bbef8ec", "47387463", "cd1bb66d", "e49245cf", "5d2989b9", "e6b6adef", "10b2d298")

# Combine data
all_data_time <- bind_rows(centralized_10, centralized_100, decentralized, decentralized_small)
all_data_cpu <- bind_rows(cpu_decentralized, cpu_centralized_10, cpu_centralized_100, cpu_decentralized_small)
m_server_data_cpu <- bind_rows(cpu_decentralized, cpu_centralized_10, cpu_centralized_100)

# Remove data points of average postgres and system cpu less than 1%
all_data_time <- all_data_time %>%
  filter(!as.character(query_hash) %in% queries_to_exclude)

agg_data_time <- all_data_time %>%
  group_by(run, system) %>%
  summarise(total_time = sum(time_ms), .groups = "drop")

# Updated color palette to match your actual system labels
cb_palette <- c(
  "10 data points/window (xxl)" = "#E63946",      # Red
  "100 data points/window (xxl)" = "#7a09d6",     # Blue
  "1000 data points/window (xxl)" = "#F77F00",    # Orange
  "Decentralized, 1 data point/window (xxl)" = "#0ffff3",  # Teal
  "Decentralized, 1 data point/window (l)" = "#018012"     # Dark Green
)

cb_shapes <- c(
  "10 data points/window (xxl)" = 18,  # diamond
  "100 data points/window (xxl)" = 16,  # square
  "1000 data points/window (xxl)" = 17,  # triangle
  "Decentralized, 1 data point/window (xxl)" = 15,  # square
  "Decentralized, 1 data point/window (l)" = 10  # small circle
)

# === PLOT ===
plot_1 <- ggplot(agg_data_time, aes(x = run, y = total_time, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  labs(x = "Benchmark Run", y = "Refresh Time (ms)", color = "System", shape = "System") +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 80000, by = 5000)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(0, 0, 0, -50)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    axis.title.y = element_text(size = 12)
 )

# cpu plots
plot_5 <- ggplot(all_data_cpu, aes(x = run, y = avg_system_cpu, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Avg. System CPU (%)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 100, by = 5)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 11)
 ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys

plot_6 <- ggplot(m_server_data_cpu, aes(x = run, y = storage_size_bytes / 1000000000, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Storage Space (GB)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 10, by = 0.2)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
 ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys


# Save the plot
png("plots/storage_plot.png", width = 2000, height = 1000, res = 350)
print(plot_6)
dev.off()

plot_7 <- ggplot(m_server_data_cpu, aes(x = run, y = bytes_received / 1000000000, color = system, shape = system)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 3) +
  labs(x = "Benchmark Run", y = "Network Transfer (GB)", color = "System", shape = "System") +
  scale_color_manual(values = cb_palette) +
  scale_shape_manual(values = cb_shapes) +
  scale_y_continuous(labels = scales::comma, breaks = seq(0, 10, by = 1)) +
  theme_minimal(base_size = 14, base_family = "Linux Libertine") +
  theme(
    plot.title = element_text(face = "bold", hjust = 0.5),
    legend.position = "top",
    legend.margin = margin(c(3, 3, 3, 3)),
    axis.text.x = element_text(size = 14),
    axis.text.y = element_text(size = 14),
    legend.text = element_text(size = 9)
 ) +
    guides(color = guide_legend(ncol = 2,
                                override.aes = list(size = 2),  # Smaller legend symbols
                                keywidth = unit(0.8, "cm"),    # Narrower legend keys
                                keyheight = unit(0.1, "cm")))  # Shorter legend keys

# Extract legend from one of the plots with common legend
get_legend <- function(myggplot){
  tmp <- ggplot_gtable(ggplot_build(myggplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}

# Get the common legend
plot_5 <- plot_5 + theme(legend.title = element_blank())
# Adjust left margin of the legend
plot_5 <- plot_5 + theme(legend.margin = margin(c(0, 0, 0, 40)))
common_legend <- get_legend(plot_5)

plot_1 <- plot_1 + theme(legend.title = element_blank(), legend.position = "none")
plot_5 <- plot_5 + theme(legend.position = "none")
plot_6 <- plot_6 + theme(legend.title = element_blank(), legend.position = "none")
plot_7 <- plot_7 + theme(legend.title = element_blank(), legend.position = "none")
plot_1 <- plot_1 + theme(axis.title.y = element_text(size = 11))
plot_5 <- plot_5 + theme(axis.title.y = element_text(size = 11))
plot_6 <- plot_6 + theme(axis.title.y = element_text(size = 11))
plot_7 <- plot_7 + theme(axis.title.y = element_text(size = 11))
plot_1 <- plot_1 + theme(axis.title.x = element_text(size = 13))
plot_5 <- plot_5 + theme(axis.title.x = element_text(size = 13))
plot_6 <- plot_6 + theme(axis.title.x = element_text(size = 13))
plot_7 <- plot_7 + theme(axis.title.x = element_text(size = 13))


png("plots/combined_plot.png", width = 2000, height = 1500, res = 350)
grid.arrange(
  arrangeGrob(plot_1, plot_5,
              plot_6, plot_7,
              ncol = 2, nrow = 2),
  common_legend,
  heights = c(10, 2)
)
dev.off()