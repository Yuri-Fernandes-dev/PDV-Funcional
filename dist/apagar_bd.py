import os
import shutil
import time

def excluir_banco_dados():
    """Exclui o arquivo de banco de dados do PDV"""
    # Localização do banco de dados
    local_app_data = os.getenv('LOCALAPPDATA')
    db_dir = os.path.join(local_app_data, 'SnapDev PDV')
    db_file = os.path.join(db_dir, 'database.db')
    
    print(f"\n=== EXCLUSÃO DO BANCO DE DADOS ===")
    print(f"Diretório: {db_dir}")
    print(f"Arquivo: {db_file}")
    
    # Verificar se o arquivo existe
    if not os.path.exists(db_file):
        print(f"\nO arquivo de banco de dados não foi encontrado em: {db_file}")
        input("\nPressione ENTER para sair...")
        return
    
    # Confirmar exclusão
    print("\nATENÇÃO: Esta operação irá remover TODOS os dados do sistema!")
    print("- Produtos")
    print("- Vendas")
    print("- Movimentos de caixa")
    print("- Usuários")
    print("\nUm novo banco de dados vazio será criado na próxima inicialização do sistema.")
    
    confirmacao = input("\nTem certeza que deseja continuar? (sim/não): ").strip().lower()
    if confirmacao != "sim":
        print("Operação cancelada pelo usuário.")
        input("\nPressione ENTER para sair...")
        return
    
    try:
        # Tentar excluir o arquivo diretamente
        print("\nExcluindo banco de dados...")
        
        # Fechar possíveis conexões
        import sqlite3
        try:
            conn = sqlite3.connect(db_file)
            conn.close()
        except:
            pass
        
        # Pequena pausa para garantir que conexões sejam fechadas
        time.sleep(1)
        
        # Excluir o arquivo
        os.remove(db_file)
        
        print("\n=== EXCLUSÃO CONCLUÍDA COM SUCESSO ===")
        print("O banco de dados foi excluído.")
        print("Na próxima inicialização do sistema, um novo banco de dados vazio será criado.")
    
    except Exception as e:
        print(f"\nERRO ao excluir o banco de dados: {str(e)}")
        
        # Tentar matar processos (Windows)
        try:
            print("\nTentando forçar o fechamento do aplicativo...")
            os.system('taskkill /F /IM "SnapDev PDV.exe" 2>nul')
            time.sleep(2)
            
            # Tentar novamente após matar processos
            if os.path.exists(db_file):
                os.remove(db_file)
                print("Banco de dados excluído com sucesso após fechar o aplicativo.")
            else:
                print("O arquivo já foi excluído.")
        except Exception as e2:
            print(f"Erro secundário: {str(e2)}")
            print("\nComo alternativa, você pode:")
            print("1. Fechar completamente o aplicativo SnapDev PDV")
            print(f"2. Excluir manualmente o arquivo: {db_file}")
    
    finally:
        input("\nPressione ENTER para sair...")

if __name__ == "__main__":
    excluir_banco_dados() 