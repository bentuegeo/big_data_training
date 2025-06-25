# Cargar paquetes necesarios
library(sf)
library(dplyr)
library(mapSpain)
library(parallel)

# 1. Obtener polígono de Zaragoza
zaragoza <- esp_get_prov(prov = "Zaragoza")

# 2. Obtener lista de archivos .gpkg
gpkg_files <- list.files("raw_data", pattern = "\\.gpkg$", full.names = TRUE)

# 3. Crear clúster (usa todos los núcleos menos uno)
cl <- makeCluster(detectCores() - 1)

# 4. Exportar objetos y cargar paquetes en cada núcleo
clusterExport(cl, varlist = c("gpkg_files", "zaragoza"))
clusterEvalQ(cl, {
  library(sf)
  library(dplyr)
})

# 5. Procesar en paralelo (sin wkt_filter, usando st_intersection)
resultados <- parLapply(cl, gpkg_files, function(file) {
  # Leer archivo completo
  datos <- st_read(file, quiet = TRUE)
  
  # Transformar Zaragoza al CRS del archivo
  zaragoza_local <- st_transform(zaragoza, st_crs(datos))
  
  # Filtrar solo puntos dentro de Zaragoza
  dentro <- st_intersection(datos, zaragoza_local)
  
  # Si no hay puntos dentro, saltar
  if (nrow(dentro) == 0) return(NULL)
  
  # Calcular medias
  data.frame(
    date = gsub("raw_data/|\\.gpkg", "", file),
    MeanTemperature = mean(dentro$MeanTemperature, na.rm = TRUE),
    Precipitation = mean(dentro$Precipitation, na.rm = TRUE),
    RelativeHumidity = mean(dentro$RelativeHumidity, na.rm = TRUE)
  )
})

# 6. Cerrar clúster
stopCluster(cl)

# 7. Combinar resultados y guardar CSV
resultados_df <- bind_rows(resultados)

dir.create("data", showWarnings = FALSE)
write.csv(resultados_df, "data/summary_zaragoza_april2025.csv", row.names = FALSE)

