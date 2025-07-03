#!/usr/bin/env python3
"""
🔄 ETL Legacy Entry Point - Compatibility Wrapper

DEPRECATED: Este archivo se mantiene para compatibilidad hacia atrás.

⚠️  USO RECOMENDADO:
    python etl/main.py                              # Entry point estándar
    python etl/main.py --tables asignaciones pagos  # Tablas específicas

🗑️  ARCHIVO LEGACY (mantener solo hasta migración completa):
    python etl/simple_incremental_etl.py

Este wrapper simplemente redirige al entry point estándar.
"""

import sys
import subprocess
from pathlib import Path


def show_deprecation_warning():
    """Mostrar advertencia de deprecación"""
    print("⚠️  DEPRECATION WARNING:")
    print("   etl/simple_incremental_etl.py está deprecated")
    print("")
    print("✅ USO RECOMENDADO:")
    print("   python etl/main.py")
    print("   python etl/main.py --tables asignaciones trandeuda")
    print("   python etl/main.py --log-level DEBUG")
    print("")
    print("🔄 Redirecting to standard entry point...")
    print("")


def main():
    """Wrapper de compatibilidad que redirige al entry point estándar"""
    show_deprecation_warning()
    
    # Construir path al entry point estándar
    current_dir = Path(__file__).parent
    main_script = current_dir / "main.py"
    
    # Pasar todos los argumentos al script estándar
    cmd = [sys.executable, str(main_script)] + sys.argv[1:]
    
    try:
        # Ejecutar el entry point estándar
        result = subprocess.run(cmd, check=False)
        sys.exit(result.returncode)
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Error executing standard entry point: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
