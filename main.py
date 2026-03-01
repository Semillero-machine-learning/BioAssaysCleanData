import os
import pandas as pd
from collections import defaultdict
from datetime import datetime
import pickle

def merge_bioassays_csvs(folder_path="BioAssays", output_csv="merged_bioassays.csv", report_file="merge_report.txt", cache_file="merge_cache.pkl"):
    """
    Mergea todos los CSVs de BioAssays, manejando columnas diferentes.
    Genera un informe detallado del proceso.
    Guarda cache cada 100 filas para poder retomar.
    """
    
    # Verificar que la carpeta existe
    if not os.path.exists(folder_path):
        print(f"Error: La carpeta '{folder_path}' no existe.")
        return
    
    # Obtener todos los archivos CSV
    csv_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.csv')])
    
    if not csv_files:
        print(f"No se encontraron archivos CSV en '{folder_path}'")
        return
    
    print(f"Se encontraron {len(csv_files)} archivos CSV")
    
    # Intentar cargar cache si existe
    cache_data = None
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
            print(f"\n✓ Cache encontrado. Retomando desde archivo {cache_data['last_file_index'] + 1}/{len(csv_files)}")
            print(f"  Filas procesadas hasta ahora: {len(cache_data['merged_df'])}")
        except Exception as e:
            print(f"⚠️  No se pudo cargar cache: {str(e)}")
            cache_data = None
    
    # Inicializar o cargar desde cache
    if cache_data:
        file_columns = cache_data['file_columns']
        all_columns = cache_data['all_columns']
        column_sources = cache_data['column_sources']
        merged_df = cache_data['merged_df']
        file_stats = cache_data['file_stats']
        row_tracking = cache_data['row_tracking']
        start_index = cache_data['last_file_index'] + 1
        rows_since_save = cache_data.get('rows_since_save', 0)
    else:
        file_columns = {}
        all_columns = set()
        column_sources = defaultdict(list)
        merged_df = pd.DataFrame()
        file_stats = {}
        row_tracking = []
        start_index = 0
        rows_since_save = 0
    
    print("\n--- Procesando archivos ---")
    
    # Procesar archivos
    for idx in range(start_index, len(csv_files)):
        filename = csv_files[idx]
        filepath = os.path.join(folder_path, filename)
        print(f"[{idx+1}/{len(csv_files)}] Procesando: {filename}")
        
        try:
            # Leer CSV, saltando las primeras dos filas
            df = pd.read_csv(filepath, skiprows=2)
            
            # Limpiar nombres de columnas
            df.columns = df.columns.str.strip()
            
            # Guardar información de columnas
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
            
            rows_since_save += len(df)
            print(f"  ✓ {filename}: {len(df)} filas añadidas (Total acumulado: {len(merged_df)} filas)")
            
            # Guardar cache cada 100 filas
            if rows_since_save >= 100:
                print(f"  💾 Guardando cache (cada 100 filas)...")
                cache_data = {
                    'file_columns': file_columns,
                    'all_columns': all_columns,
                    'column_sources': dict(column_sources),
                    'merged_df': merged_df,
                    'file_stats': file_stats,
                    'row_tracking': row_tracking,
                    'last_file_index': idx,
                    'rows_since_save': 0
                }
                try:
                    with open(cache_file, 'wb') as f:
                        pickle.dump(cache_data, f)
                    print(f"  ✓ Cache guardado exitosamente")
                except Exception as e:
                    print(f"  ⚠️  Error al guardar cache: {str(e)}")
                
                rows_since_save = 0
            
        except Exception as e:
            print(f"  ⚠️  Error al procesar {filename}: {str(e)}")
            continue
    
    print(f"\n✓ Total de columnas únicas encontradas: {len(all_columns)}")
    
    # Identificar columnas comunes y no comunes
    if len(file_columns) > 0:
        common_columns = set.intersection(*[cols for cols in file_columns.values()])
        uncommon_columns = all_columns - common_columns
    else:
        common_columns = set()
        uncommon_columns = set()
    
    print(f"✓ Columnas comunes a todos los archivos: {len(common_columns)}")
    print(f"✓ Columnas que varían entre archivos: {len(uncommon_columns)}")
    
    # Guardar CSV mergeado
    print(f"\n--- Guardando archivo final ---")
    try:
        merged_df.to_csv(output_csv, index=False)
        print(f"✓ Archivo mergeado guardado: {output_csv}")
        print(f"  Total de filas: {len(merged_df)}")
        print(f"  Total de columnas: {len(merged_df.columns)}")
    except Exception as e:
        print(f"❌ Error al guardar CSV: {str(e)}")
        return
    
    # Generar informe detallado
    print(f"\n--- Generando informe ---")
    
    try:
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
        
    except Exception as e:
        print(f"❌ Error al generar informe: {str(e)}")
        return
    
    # Eliminar cache al terminar exitosamente
    if os.path.exists(cache_file):
        try:
            os.remove(cache_file)
            print(f"✓ Cache eliminado (proceso completado)")
        except:
            pass
    
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