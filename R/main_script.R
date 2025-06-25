dir.create("R")
dir.create("raw_data")
dir.create("data")
dir.create("outputs")
dir.create("results")

# Cargar paquetes necesarios
library(data.table)
library(purrr)
library(furrr)
library(sf)
library(parallel)
library(arrow)
