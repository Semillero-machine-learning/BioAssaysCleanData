import os
import pandas as pd
from collections import defaultdict
from datetime import datetime

def merge_bioassays_csvs(folder_path="BioAssays", output_csv="merged_bioassays.csv", report_file="merge_report.txt"):
    """
    Mergea todos los CSVs de BioAssays, manejando columnas diferentes.
    Genera un informe detallado del proceso.
    """
    
    # Verificar que la carpeta existe
    if not os.path.exists(folder_path):
        print(f"Error: La carpeta '{folder_path}' no existe.")
        return
    
    # Obtener todos los archivos CSV
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No se encontraron archivos CSV en '{folder_path}'")
        return
    
    print(f"Se encontraron {len(csv_files)} archivos CSV")
    
    # Diccionarios para tracking
    file_columns = {}  # {filename: set of columns}
    all_columns = set()
    column_sources = defaultdict(list)  # {column: [files that have it]}
    dataframes = []
    file_stats = {}
    
    # Primera pasada: leer todos los archivos y analizar columnas
    print("\n--- Fase 1: Analizando estructura de archivos ---")
    
    for idx, filename in enumerate(csv_files, 1):
        filepath = os.path.join(folder_path, filename)
        print(f"[{idx}/{len(csv_files)}] Procesando: {filename}")
        
        try:
            # Leer CSV, saltando las primeras dos filas (RESULT_TYPE y RESULT_DESCR)
            df = pd.read_csv(filepath, skiprows=2)
            
            # Limpiar nombres de columnas (remover espacios)
            df.columns = df.columns.str.strip()
            
            # Guardar información
            cols = set(df.columns)
            file_columns[filename] = cols
            all_columns.update(cols)
            
            for col in cols:
                column_sources[col].append(filename)
            
            file_stats[filename] = {
                'rows': len(df),
                'columns': len(cols),
                'column_list': sorted(list(cols))
            }
            
            dataframes.append((filename, df))
            
        except Exception as e:
            print(f"  ⚠️  Error al procesar {filename}: {str(e)}")
            continue
    
    print(f"\n✓ Total de columnas únicas encontradas: {len(all_columns)}")
    
    # Identificar columnas que no están en todos los archivos
    common_columns = set.intersection(*[cols for cols in file_columns.values()])
    uncommon_columns = all_columns - common_columns
    
    print(f"✓ Columnas comunes a todos los archivos: {len(common_columns)}")
    print(f"✓ Columnas que varían entre archivos: {len(uncommon_columns)}")
    
    # Segunda pasada: mergear con todas las columnas
    print("\n--- Fase 2: Mergeando archivos ---")
    
    merged_df = pd.DataFrame()
    row_tracking = []  # Para saber de qué archivo viene cada fila
    
    for filename, df in dataframes:
        # Añadir columnas faltantes con NaN
        for col in all_columns:
            if col not in df.columns:
                df[col] = pd.NA
        
        # Reordenar columnas para consistencia
        df = df[sorted(all_columns)]
        
        # Track de filas
        start_row = len(merged_df)
        merged_df = pd.concat([merged_df, df], ignore_index=True)
        end_row = len(merged_df) - 1
        
        row_tracking.append({
            'filename': filename,
            'start_row': start_row,
            'end_row': end_row,
            'num_rows': len(df)
        })
        
        print(f"  ✓ {filename}: {len(df)} filas añadidas")
    
    # Guardar CSV mergeado
    merged_df.to_csv(output_csv, index=False)
    print(f"\n✓ Archivo mergeado guardado: {output_csv}")
    print(f"  Total de filas: {len(merged_df)}")
    print(f"  Total de columnas: {len(merged_df.columns)}")
    
    # Generar informe detallado
    print(f"\n--- Fase 3: Generando informe ---")
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("INFORME DETALLADO DE MERGE DE BIOASSAYS\n")
        f.write("="*80 + "\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Carpeta procesada: {folder_path}\n")
        f.write(f"Archivo de salida: {output_csv}\n")
        f.write("\n")
        
        # Resumen general
        f.write("="*80 + "\n")
        f.write("1. RESUMEN GENERAL\n")
        f.write("="*80 + "\n")
        f.write(f"Total de archivos procesados: {len(csv_files)}\n")
        f.write(f"Total de filas en el merge: {len(merged_df)}\n")
        f.write(f"Total de columnas únicas: {len(all_columns)}\n")
        f.write(f"Columnas comunes a todos: {len(common_columns)}\n")
        f.write(f"Columnas variables: {len(uncommon_columns)}\n")
        f.write("\n")
        
        # Columnas comunes
        f.write("="*80 + "\n")
        f.write("2. COLUMNAS COMUNES A TODOS LOS ARCHIVOS\n")
        f.write("="*80 + "\n")
        for col in sorted(common_columns):
            f.write(f"  - {col}\n")
        f.write("\n")
        
        # Columnas variables y sus fuentes
        f.write("="*80 + "\n")
        f.write("3. COLUMNAS ADICIONALES (NO PRESENTES EN TODOS LOS ARCHIVOS)\n")
        f.write("="*80 + "\n")
        
        for col in sorted(uncommon_columns):
            files_with_col = column_sources[col]
            f.write(f"\nColumna: {col}\n")
            f.write(f"  Presente en {len(files_with_col)} de {len(csv_files)} archivos ({len(files_with_col)/len(csv_files)*100:.1f}%)\n")
            f.write(f"  Archivos que la contienen:\n")
            for fname in files_with_col:
                f.write(f"    - {fname}\n")
        f.write("\n")
        
        # Archivos sin coincidencia completa
        f.write("="*80 + "\n")
        f.write("4. ARCHIVOS CON COLUMNAS DIFERENTES AL CONJUNTO COMPLETO\n")
        f.write("="*80 + "\n")
        
        for filename, cols in file_columns.items():
            missing_cols = all_columns - cols
            extra_cols = cols - common_columns
            
            if missing_cols or extra_cols:
                f.write(f"\n{filename}:\n")
                f.write(f"  Total de columnas: {len(cols)}\n")
                
                if missing_cols:
                    f.write(f"  Columnas faltantes ({len(missing_cols)}):\n")
                    for col in sorted(missing_cols):
                        f.write(f"    - {col}\n")
                
                if extra_cols:
                    f.write(f"  Columnas adicionales no comunes ({len(extra_cols)}):\n")
                    for col in sorted(extra_cols):
                        f.write(f"    - {col}\n")
        f.write("\n")
        
        # Detalles de cada archivo
        f.write("="*80 + "\n")
        f.write("5. DETALLES POR ARCHIVO\n")
        f.write("="*80 + "\n")
        
        for filename, stats in file_stats.items():
            f.write(f"\n{filename}:\n")
            f.write(f"  Filas originales: {stats['rows']}\n")
            f.write(f"  Columnas: {stats['columns']}\n")
            f.write(f"  Lista de columnas:\n")
            for col in stats['column_list']:
                f.write(f"    - {col}\n")
        f.write("\n")
        
        # Mapping de filas
        f.write("="*80 + "\n")
        f.write("6. MAPEO DE FILAS EN EL ARCHIVO MERGEADO\n")
        f.write("="*80 + "\n")
        f.write("(Filas indexadas desde 0 en el archivo final)\n\n")
        
        for track in row_tracking:
            f.write(f"{track['filename']}:\n")
            f.write(f"  Filas {track['start_row']} - {track['end_row']} ({track['num_rows']} filas)\n")
        f.write("\n")
        
        # Columnas añadidas por rango de filas
        f.write("="*80 + "\n")
        f.write("7. COLUMNAS ADICIONALES POR RANGO DE FILAS\n")
        f.write("="*80 + "\n")
        f.write("(Qué columnas fueron añadidas como NaN para qué filas)\n\n")
        
        for track in row_tracking:
            filename = track['filename']
            cols_in_file = file_columns[filename]
            added_cols = all_columns - cols_in_file
            
            if added_cols:
                f.write(f"\n{filename} (filas {track['start_row']}-{track['end_row']}):\n")
                f.write(f"  Se añadieron {len(added_cols)} columnas con valores NaN:\n")
                for col in sorted(added_cols):
                    f.write(f"    - {col}\n")
        
        f.write("\n")
        f.write("="*80 + "\n")
        f.write("FIN DEL INFORME\n")
        f.write("="*80 + "\n")
    
    print(f"✓ Informe generado: {report_file}")
    print("\n¡Proceso completado exitosamente!")
    
    # Mostrar resumen final
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    print(f"Archivos procesados: {len(csv_files)}")
    print(f"Filas totales: {len(merged_df)}")
    print(f"Columnas totales: {len(all_columns)}")
    if uncommon_columns:
        print(f"\n⚠️  Hay {len(uncommon_columns)} columnas que no están en todos los archivos")
        print("   Revisar el informe para más detalles")
    else:
        print("\n✓ Todos los archivos tienen las mismas columnas")

# Ejecutar el merge
if __name__ == "__main__":
    merge_bioassays_csvs()