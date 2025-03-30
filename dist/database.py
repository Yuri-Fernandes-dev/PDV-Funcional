import os
import json
import sqlite3
from datetime import datetime
import hashlib
import pytz  # Adicionar para suporte a fusos horários
import platform

def get_app_data_dir():
    """Retorna o diretório de dados da aplicação baseado no sistema operacional"""
    if platform.system() == 'Windows':
        app_data = os.path.join(os.getenv('LOCALAPPDATA'), 'SnapDev PDV')
    else:
        app_data = os.path.expanduser('~/snapdev_pdv')
    
    # Criar diretório se não existir
    os.makedirs(app_data, exist_ok=True)
    return app_data

def get_database_path():
    """Retorna o caminho completo do arquivo do banco de dados"""
    return os.path.join(get_app_data_dir(), 'database.db')

def get_connection():
    """Retorna uma conexão com o banco de dados SQLite"""
    conn = sqlite3.connect(get_database_path())
    conn.row_factory = sqlite3.Row  # Para acessar colunas pelo nome
    return conn

def get_datetime_now():
    """Retorna a data e hora atual no formato de Brasília"""
    # Configurar timezone para Brasília
    tz_brasil = pytz.timezone('America/Sao_Paulo')
    return datetime.now(tz_brasil).strftime("%Y-%m-%d %H:%M:%S")

def create_database():
    """Cria o banco de dados e suas tabelas se não existirem"""
    db_path = get_database_path()
    
    # Criar conexão com o banco de dados
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Criar tabela de usuários
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        usuario TEXT UNIQUE NOT NULL,
        senha TEXT NOT NULL,
        tipo TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Criar tabela de produtos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT UNIQUE NOT NULL,
        nome TEXT NOT NULL,
        descricao TEXT,
        preco REAL NOT NULL,
        quantidade INTEGER NOT NULL DEFAULT 0,
        min_quantidade INTEGER NOT NULL DEFAULT 0,
        categoria TEXT,
        marca TEXT,
        tamanho TEXT,
        cor TEXT,
        imagem TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Criar tabela de vendas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vendas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER NOT NULL,
        valor_total REAL NOT NULL,
        forma_pagamento TEXT NOT NULL,
        data_venda TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
    )
    ''')
    
    # Criar tabela de itens da venda
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS itens_venda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venda_id INTEGER NOT NULL,
        produto_id INTEGER NOT NULL,
        quantidade INTEGER NOT NULL,
        preco_unitario REAL NOT NULL,
        subtotal REAL NOT NULL,
        FOREIGN KEY (venda_id) REFERENCES vendas (id),
        FOREIGN KEY (produto_id) REFERENCES produtos (id)
    )
    ''')
    
    # Criar tabela de movimentações de estoque
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movimentacoes_estoque (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        produto_id INTEGER NOT NULL,
        quantidade INTEGER NOT NULL,
        tipo_movimento TEXT NOT NULL,
        referencia TEXT,
        data_movimento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (produto_id) REFERENCES produtos (id)
    )
    ''')
    
    # Criar tabela de caixa
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS caixa (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        saldo_inicial REAL NOT NULL DEFAULT 0,
        saldo_atual REAL NOT NULL DEFAULT 0,
        ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Criar tabela de movimentos do caixa
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movimentos_caixa (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        tipo TEXT NOT NULL,
        descricao TEXT,
        valor REAL NOT NULL
    )
    ''')
    
    # Criar usuário admin se não existir
    cursor.execute('SELECT id FROM usuarios WHERE usuario = ?', ('admin',))
    if not cursor.fetchone():
        cursor.execute('''
        INSERT INTO usuarios (nome, usuario, senha, tipo)
        VALUES (?, ?, ?, ?)
        ''', ('Administrador', 'admin', 'admin', 'admin'))
    
    # Commit e fechar conexão
    conn.commit()
    conn.close()

def check_and_fix_database():
    """Verifica e corrige problemas na estrutura do banco de dados"""
    conn = sqlite3.connect(get_database_path())
    cursor = conn.cursor()
    
    try:
        # Verificar se a tabela produtos existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='produtos'")
        if cursor.fetchone():
            # Verificar se a coluna marca existe
            cursor.execute("PRAGMA table_info(produtos)")
            colunas = [coluna[1] for coluna in cursor.fetchall()]
            
            # Se a coluna marca não existir, adicionar
            if "marca" not in colunas:
                try:
                    cursor.execute("ALTER TABLE produtos ADD COLUMN marca TEXT")
                    print("Coluna 'marca' adicionada com sucesso!")
                    conn.commit()
                except Exception as e:
                    print(f"Erro ao adicionar coluna 'marca': {str(e)}")
            
            # Se a coluna cor não existir, adicionar
            if "cor" not in colunas:
                try:
                    cursor.execute("ALTER TABLE produtos ADD COLUMN cor TEXT")
                    print("Coluna 'cor' adicionada com sucesso!")
                    conn.commit()
                except Exception as e:
                    print(f"Erro ao adicionar coluna 'cor': {str(e)}")
                    
            # Se a coluna tamanho não existir, adicionar
            if "tamanho" not in colunas:
                try:
                    cursor.execute("ALTER TABLE produtos ADD COLUMN tamanho TEXT")
                    print("Coluna 'tamanho' adicionada com sucesso!")
                    conn.commit()
                except Exception as e:
                    print(f"Erro ao adicionar coluna 'tamanho': {str(e)}")
        
        # Verificar se a tabela vendas existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'")
        if cursor.fetchone():
            # Verificar colunas necessárias na tabela vendas
            cursor.execute("PRAGMA table_info(vendas)")
            colunas_vendas = [coluna[1] for coluna in cursor.fetchall()]
            
            # Verificar colunas necessárias para o processo de vendas
            colunas_necessarias = {
                'usuario_id': 'INTEGER',
                'valor_total': 'REAL',
                'forma_pagamento': 'TEXT',
                'desconto': 'REAL',
                'data_venda': 'TIMESTAMP',
                'codigo': 'TEXT'
            }
            
            # Adicionar colunas que faltam
            for coluna, tipo in colunas_necessarias.items():
                if coluna not in colunas_vendas:
                    try:
                        cursor.execute(f"ALTER TABLE vendas ADD COLUMN {coluna} {tipo}")
                        print(f"Coluna '{coluna}' adicionada à tabela vendas com sucesso!")
                        conn.commit()
                    except Exception as e:
                        print(f"Erro ao adicionar coluna '{coluna}' à tabela vendas: {str(e)}")
                    
        print("Verificação da estrutura do banco de dados concluída.")
        
    except Exception as e:
        print(f"Erro ao verificar/corrigir banco de dados: {str(e)}")
    finally:
        conn.close()

def init_db():
    """Inicializa o banco de dados"""
    # Se o banco de dados não existir, cria-o
    if not os.path.exists(get_database_path()):
        create_database()
    
    # Verificar e corrigir a estrutura do banco de dados
    check_and_fix_database()

def get_db_connection():
    """Retorna uma conexão com o banco de dados"""
    db_path = get_database_path()
    return sqlite3.connect(db_path)

def get_db_cursor():
    """Retorna um cursor para executar comandos no banco de dados"""
    conn = get_db_connection()
    return conn.cursor()

def close_db_connection(conn):
    """Fecha a conexão com o banco de dados"""
    if conn:
        conn.close()

def execute_query(query, params=None):
    """Executa uma query SQL e retorna o resultado"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        result = cursor.fetchall()
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def execute_many(query, params_list):
    """Executa várias queries SQL com diferentes parâmetros"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.executemany(query, params_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_user_by_username(username):
    """Retorna um usuário pelo nome de usuário"""
    query = "SELECT * FROM users WHERE username = ?"
    result = execute_query(query, (username,))
    return result[0] if result else None

def get_product_by_code(code):
    """Retorna um produto pelo código"""
    query = "SELECT * FROM products WHERE code = ?"
    result = execute_query(query, (code,))
    return result[0] if result else None

def get_product_by_id(product_id):
    """Retorna um produto pelo ID"""
    query = "SELECT * FROM products WHERE id = ?"
    result = execute_query(query, (product_id,))
    return result[0] if result else None

def get_all_products():
    """Retorna todos os produtos"""
    query = "SELECT * FROM products ORDER BY name"
    return execute_query(query)

def get_products_by_category(category):
    """Retorna produtos de uma categoria específica"""
    query = "SELECT * FROM products WHERE category = ? ORDER BY name"
    return execute_query(query, (category,))

def get_categories():
    """Retorna todas as categorias de produtos"""
    query = "SELECT DISTINCT category FROM products WHERE category IS NOT NULL ORDER BY category"
    return execute_query(query)

def add_product(code, name, description, price, stock, min_stock, category, image_path=None):
    """Adiciona um novo produto"""
    query = '''
    INSERT INTO products (code, name, description, price, stock, min_stock, category, image_path)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    params = (code, name, description, price, stock, min_stock, category, image_path)
    execute_query(query, params)

def update_product(product_id, code, name, description, price, stock, min_stock, category, image_path=None):
    """Atualiza um produto existente"""
    query = '''
    UPDATE products 
    SET code = ?, name = ?, description = ?, price = ?, stock = ?, min_stock = ?, category = ?, image_path = ?, updated_at = CURRENT_TIMESTAMP
    WHERE id = ?
    '''
    params = (code, name, description, price, stock, min_stock, category, image_path, product_id)
    execute_query(query, params)

def delete_product(product_id):
    """Remove um produto"""
    query = "DELETE FROM products WHERE id = ?"
    execute_query(query, (product_id,))

def update_stock(product_id, quantity, movement_type, reference=None):
    """Atualiza o estoque de um produto"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Atualizar estoque do produto
        cursor.execute('''
        UPDATE products 
        SET stock = stock + ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        ''', (quantity, product_id))
        
        # Registrar movimentação
        cursor.execute('''
        INSERT INTO stock_movements (product_id, quantity, movement_type, reference)
        VALUES (?, ?, ?, ?)
        ''', (product_id, quantity, movement_type, reference))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def create_sale(user_id, items, payment_method):
    """Cria uma nova venda"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Calcular valor total
        total_amount = sum(item['quantity'] * item['unit_price'] for item in items)
        
        # Criar venda
        cursor.execute('''
        INSERT INTO sales (user_id, total_amount, payment_method)
        VALUES (?, ?, ?)
        ''', (user_id, total_amount, payment_method))
        
        sale_id = cursor.lastrowid
        
        # Adicionar itens da venda
        for item in items:
            cursor.execute('''
            INSERT INTO sale_items (sale_id, product_id, quantity, unit_price, total_price)
            VALUES (?, ?, ?, ?, ?)
            ''', (sale_id, item['product_id'], item['quantity'], item['unit_price'], item['total_price']))
            
            # Atualizar estoque
            cursor.execute('''
            UPDATE products 
            SET stock = stock - ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (item['quantity'], item['product_id']))
        
        conn.commit()
        return sale_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_sales_by_date_range(start_date, end_date):
    """Retorna vendas em um intervalo de datas"""
    query = '''
    SELECT s.*, u.name as user_name
    FROM sales s
    JOIN users u ON s.user_id = u.id
    WHERE s.created_at BETWEEN ? AND ?
    ORDER BY s.created_at DESC
    '''
    return execute_query(query, (start_date, end_date))

def get_sale_items(sale_id):
    """Retorna os itens de uma venda"""
    query = '''
    SELECT si.*, p.code, p.name
    FROM sale_items si
    JOIN products p ON si.product_id = p.id
    WHERE si.sale_id = ?
    '''
    return execute_query(query, (sale_id,))

def get_stock_movements(product_id=None, start_date=None, end_date=None):
    """Retorna movimentações de estoque"""
    query = '''
    SELECT sm.*, p.code, p.name
    FROM stock_movements sm
    JOIN products p ON sm.product_id = p.id
    WHERE 1=1
    '''
    params = []
    
    if product_id:
        query += " AND sm.product_id = ?"
        params.append(product_id)
    
    if start_date:
        query += " AND sm.created_at >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND sm.created_at <= ?"
        params.append(end_date)
    
    query += " ORDER BY sm.created_at DESC"
    
    return execute_query(query, tuple(params) if params else None)

def get_low_stock_products():
    """Retorna produtos com estoque abaixo do mínimo"""
    query = '''
    SELECT * FROM products 
    WHERE stock <= min_stock 
    ORDER BY (stock - min_stock) ASC
    '''
    return execute_query(query)

def get_sales_summary(start_date, end_date):
    """Retorna um resumo das vendas em um período"""
    query = '''
    SELECT 
        COUNT(*) as total_sales,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as average_sale
    FROM sales
    WHERE created_at BETWEEN ? AND ?
    '''
    return execute_query(query, (start_date, end_date))[0]

def get_top_products(limit=10, start_date=None, end_date=None):
    """Retorna os produtos mais vendidos"""
    query = '''
    SELECT 
        p.id,
        p.code,
        p.name,
        SUM(si.quantity) as total_quantity,
        SUM(si.total_price) as total_revenue
    FROM sale_items si
    JOIN products p ON si.product_id = p.id
    JOIN sales s ON si.sale_id = s.id
    WHERE 1=1
    '''
    params = []
    
    if start_date:
        query += " AND s.created_at >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND s.created_at <= ?"
        params.append(end_date)
    
    query += '''
    GROUP BY p.id
    ORDER BY total_quantity DESC
    LIMIT ?
    '''
    params.append(limit)
    
    return execute_query(query, tuple(params))

def hash_password(password):
    """Cria um hash seguro da senha"""
    return hashlib.sha256(password.encode()).hexdigest()

def get_produtos():
    """Retorna a lista de produtos"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM produtos")
    produtos = []
    
    for row in cursor.fetchall():
        produto = dict(row)
        produtos.append(produto)
    
    conn.close()
    return produtos

def save_produtos(produtos):
    """Salva a lista de produtos (compatibilidade com código anterior)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for produto in produtos:
        if "id" in produto and produto["id"]:
            # Atualizar produto existente
            cursor.execute('''
            UPDATE produtos SET 
                codigo = ?,
                nome = ?,
                descricao = ?,
                categoria = ?,
                tamanho = ?,
                cor = ?,
                preco = ?,
                quantidade = ?,
                imagem = ?
            WHERE id = ?
            ''', (
                produto["codigo"],
                produto["nome"],
                produto.get("descricao", ""),
                produto.get("categoria", ""),
                produto["tamanho"],
                produto["cor"],
                produto["preco"],
                produto["quantidade"],
                produto.get("imagem", ""),
                produto["id"]
            ))
        else:
            # Inserir novo produto
            cursor.execute('''
            INSERT INTO produtos 
            (codigo, nome, descricao, categoria, tamanho, cor, preco, quantidade, imagem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                produto["codigo"],
                produto["nome"],
                produto.get("descricao", ""),
                produto.get("categoria", ""),
                produto["tamanho"],
                produto["cor"],
                produto["preco"],
                produto["quantidade"],
                produto.get("imagem", "")
            ))
            # Se o produto for novo e tiver um ID específico definido, atualizamos
            if "id" in produto:
                last_id = cursor.lastrowid
                cursor.execute("UPDATE produtos SET id = ? WHERE id = ?", (produto["id"], last_id))
    
    conn.commit()
    conn.close()

def add_produto(produto):
    """Adiciona um novo produto"""
    conn = sqlite3.connect(get_database_path())
    cursor = conn.cursor()
    
    try:
        # Verificar se já existe produto com o mesmo código
        cursor.execute("SELECT id FROM produtos WHERE codigo = ?", (produto["codigo"],))
        existing = cursor.fetchone()
        
        if existing:
            return -1, "Já existe um produto com este código"
        
        # Inserir o produto
        cursor.execute('''
        INSERT INTO produtos 
            (codigo, nome, descricao, preco, quantidade, categoria, marca, tamanho, cor, imagem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            produto["codigo"],
            produto["nome"],
            produto.get("descricao", ""),
            produto["preco"],
            produto["quantidade"],
            produto.get("categoria", ""),
            produto.get("marca", ""),
            produto.get("tamanho", ""),
            produto.get("cor", ""),
            produto.get("imagem", "")
        ))
        
        produto_id = cursor.lastrowid
        conn.commit()
        
        return produto_id, "Produto cadastrado com sucesso!"
    except Exception as e:
        conn.rollback()
        print(f"Erro ao adicionar produto: {str(e)}")
        return -1, f"Não foi possível cadastrar o produto: {str(e)}"
    finally:
        conn.close()

def update_produto(produto):
    """Atualiza um produto existente"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    UPDATE produtos SET 
        codigo = ?,
        nome = ?,
        descricao = ?,
        categoria = ?,
        tamanho = ?,
        cor = ?,
        preco = ?,
        quantidade = ?,
        imagem = ?
    WHERE id = ?
    ''', (
        produto["codigo"],
        produto["nome"],
        produto.get("descricao", ""),
        produto.get("categoria", ""),
        produto["tamanho"],
        produto.get("cor", ""),
        produto["preco"],
        produto["quantidade"],
        produto.get("imagem", ""),
        produto["id"]
    ))
    
    conn.commit()
    conn.close()
    return True

def delete_produto(id):
    """Exclui um produto pelo ID"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM produtos WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        return True, "Produto excluído com sucesso"
    except Exception as e:
        conn.rollback()
        conn.close()
        return False, f"Erro ao excluir produto: {str(e)}"

def get_vendas():
    """Retorna a lista de vendas"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar se a coluna data_venda existe
    cursor.execute("PRAGMA table_info(vendas)")
    colunas = cursor.fetchall()
    tem_data_venda = any(coluna[1] == 'data_venda' for coluna in colunas)
    
    # Buscar vendas
    try:
        if tem_data_venda:
            cursor.execute('SELECT * FROM vendas ORDER BY data_venda DESC')
        else:
            # Caso a coluna não exista, tentar usar 'data' (nome antigo)
            cursor.execute('SELECT * FROM vendas ORDER BY data DESC')
    except sqlite3.OperationalError:
        # Erro na consulta - tentar ordenar pelo ID
        cursor.execute('SELECT * FROM vendas ORDER BY id DESC')
    
    vendas_rows = cursor.fetchall()
    
    vendas = []
    for venda_row in vendas_rows:
        venda = dict(venda_row)
        
        # Buscar itens da venda
        cursor.execute('''
        SELECT * FROM itens_venda WHERE venda_id = ?
        ''', (venda["id"],))
        
        itens = [dict(item) for item in cursor.fetchall()]
        venda["itens"] = itens
        
        vendas.append(venda)
    
    conn.close()
    return vendas

def registrar_venda(venda, valor_total=None, forma_pagamento=None, desconto=0, valor_recebido=0, troco=0):
    """Registra uma venda no banco de dados
    
    Args:
        venda: Pode ser uma lista de itens da venda ou um dicionário com os dados da venda
        valor_total: Valor total da venda (opcional, pode vir no objeto venda)
        forma_pagamento: Forma de pagamento (opcional, pode vir no objeto venda)
        desconto: Valor do desconto (opcional)
        valor_recebido: Valor recebido do cliente (opcional)
        troco: Valor do troco (opcional)
        
    Returns:
        int: ID da venda registrada
    """
    try:
        conn = sqlite3.connect(get_database_path())
        cursor = conn.cursor()
        
        # Verificar colunas da tabela vendas
        cursor.execute("PRAGMA table_info(vendas)")
        colunas = [coluna[1] for coluna in cursor.fetchall()]
        
        # Se recebermos um objeto venda, extrai os valores dele
        if isinstance(venda, dict):
            # Usar valores do objeto venda se fornecidos
            codigo = venda.get("codigo", f"V{datetime.now().strftime('%Y%m%d%H%M%S')}")
            subtotal = venda.get("subtotal", 0)
            if valor_total is None:
                valor_total = venda.get("total", subtotal)
            if forma_pagamento is None:
                forma_pagamento = venda.get("forma_pagamento", "Dinheiro")
            desconto = venda.get("desconto", desconto)
            valor_recebido = venda.get("valor_recebido", valor_recebido)
            troco = venda.get("troco", troco)
            itens_venda = venda.get("itens", [])
        else:
            # Se não for um dicionário, usamos os parâmetros individuais
            # e venda é tratado como itens_venda
            itens_venda = venda
            codigo = f"V{datetime.now().strftime('%Y%m%d%H%M%S')}"
            subtotal = valor_total
        
        # Obter ID do usuário atual (para simplificar, usando 1 como padrão)
        usuario_id = 1
        
        # Construir a consulta SQL de acordo com as colunas disponíveis
        campos = []
        valores = []
        valores_placeholders = []
        
        # Sempre incluir estas colunas básicas
        if 'usuario_id' in colunas:
            campos.append('usuario_id')
            valores.append(usuario_id)
            valores_placeholders.append('?')
            
        if 'valor_total' in colunas:
            campos.append('valor_total')
            valores.append(valor_total)
            valores_placeholders.append('?')
            
        if 'forma_pagamento' in colunas:
            campos.append('forma_pagamento')
            valores.append(forma_pagamento)
            valores_placeholders.append('?')
            
        if 'codigo' in colunas:
            campos.append('codigo')
            valores.append(codigo)
            valores_placeholders.append('?')
            
        if 'desconto' in colunas:
            campos.append('desconto')
            valores.append(desconto)
            valores_placeholders.append('?')
        
        # Adicionar coluna de data se existir
        if 'data_venda' in colunas:
            campos.append('data_venda')
            valores.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            valores_placeholders.append('?')
        
        # Criar a consulta SQL
        sql = f"""
        INSERT INTO vendas 
            ({', '.join(campos)})
        VALUES
            ({', '.join(valores_placeholders)})
        """
        
        cursor.execute(sql, valores)
        venda_id = cursor.lastrowid
        
        # Verificar se a tabela itens_venda existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='itens_venda'")
        tem_tabela_itens = cursor.fetchone() is not None
        
        if tem_tabela_itens and itens_venda:
            # Registrar itens da venda na tabela itens_venda
            for item in itens_venda:
                # Obter produto pelo código
                codigo_produto = item["codigo"]
                cursor.execute("SELECT id FROM produtos WHERE codigo = ?", (codigo_produto,))
                produto = cursor.fetchone()
                
                if produto:
                    produto_id = produto[0]
                    cursor.execute('''
                    INSERT INTO itens_venda 
                        (venda_id, produto_id, quantidade, preco_unitario, subtotal)
                    VALUES
                        (?, ?, ?, ?, ?)
                    ''', (
                        venda_id,
                        produto_id,
                        item["quantidade"],
                        item["preco"],
                        item["subtotal"]
                    ))
                    
                    # Atualizar estoque do produto
                    cursor.execute('''
                    UPDATE produtos 
                    SET quantidade = quantidade - ?
                    WHERE id = ?
                    ''', (item["quantidade"], produto_id))
        
        conn.commit()
        return venda_id
    except Exception as e:
        print(f"Erro ao registrar venda: {str(e)}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def get_relatorio_vendas(data_inicio=None, data_fim=None):
    """Retorna as vendas realizadas em um determinado período"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar se a coluna data_venda existe
    cursor.execute("PRAGMA table_info(vendas)")
    colunas = cursor.fetchall()
    tem_data_venda = any(coluna[1] == 'data_venda' for coluna in colunas)
    coluna_data = 'data_venda' if tem_data_venda else 'data'
    
    query = "SELECT * FROM vendas"
    params = []
    
    if data_inicio or data_fim:
        query += " WHERE "
        
        if data_inicio:
            query += f"{coluna_data} >= ?"
            params.append(data_inicio)
            
            if data_fim:
                query += " AND "
        
        if data_fim:
            query += f"{coluna_data} <= ?"
            # Garantir que inclua todo o último dia
            if not ' ' in data_fim:
                data_fim = data_fim + " 23:59:59"
            params.append(data_fim)
    
    query += f" ORDER BY {coluna_data} DESC"
    
    try:
        cursor.execute(query, params)
    except sqlite3.OperationalError as e:
        print(f"Erro ao buscar vendas: {str(e)}")
        # Se ocorrer erro, tenta consulta sem filtro por data
        try:
            cursor.execute("SELECT * FROM vendas ORDER BY id DESC")
        except sqlite3.OperationalError:
            # Se ainda ocorrer erro, pode ser que a tabela não exista
            return []
    
    vendas_rows = cursor.fetchall()
    
    vendas = []
    for venda_row in vendas_rows:
        venda = dict(venda_row)
        
        # Buscar itens da venda
        try:
            cursor.execute('''
            SELECT * FROM itens_venda WHERE venda_id = ?
            ''', (venda["id"],))
            
            itens = [dict(item) for item in cursor.fetchall()]
            venda["itens"] = itens
        except sqlite3.OperationalError:
            # Se não encontrar a tabela de itens
            venda["itens"] = []
        
        vendas.append(venda)
    
    conn.close()
    return vendas

def get_caixa():
    """Retorna os dados do caixa atual"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM caixa ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    
    # Se não há caixa, criar um caixa fechado
    if not result:
        cursor.execute('''
        INSERT INTO caixa 
        (saldo_inicial, saldo_atual, ultima_atualizacao)
        VALUES (?, ?, ?)
        ''', (0, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        
        cursor.execute("SELECT * FROM caixa ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
    
    caixa = dict(result)
    
    # Buscar movimentações
    cursor.execute("SELECT * FROM movimentos_caixa ORDER BY data DESC LIMIT 100")
    movimentacoes = [dict(mov) for mov in cursor.fetchall()]
    
    caixa["movimentacoes"] = movimentacoes
    
    # Determinar o status do caixa baseado nas movimentações
    caixa["status"] = "fechado"  # Status padrão é fechado
    
    if movimentacoes:
        # Procurar pela última movimentação de abertura ou fechamento
        for mov in movimentacoes:
            if "abertura de caixa" in mov["descricao"].lower():
                caixa["status"] = "aberto"
                break
            elif "fechamento de caixa" in mov["descricao"].lower():
                caixa["status"] = "fechado"
                break
        
        # Se tiver movimentações e o saldo atual for maior que zero, considerar aberto
        if caixa["status"] == "fechado" and caixa["saldo_atual"] > 0:
            caixa["status"] = "aberto"
    
    conn.close()
    return caixa

def get_movimentos_caixa():
    """Retorna as movimentações do caixa"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM movimentos_caixa ORDER BY data DESC")
        movimentos = [dict(row) for row in cursor.fetchall()]
        return movimentos
    except sqlite3.OperationalError as e:
        print(f"Erro ao buscar movimentos do caixa: {str(e)}")
        return []
    finally:
        conn.close()

def registrar_movimento_caixa(tipo, descricao, valor):
    """Registra um movimento no caixa (entrada ou saída)"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Inserir movimento
        cursor.execute('''
        INSERT INTO movimentos_caixa 
        (data, tipo, descricao, valor)
        VALUES (?, ?, ?, ?)
        ''', (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            tipo,
            descricao,
            valor
        ))
        
        movimento_id = cursor.lastrowid
        
        # Atualizar saldo do caixa
        if tipo == "entrada":
            cursor.execute('''
            UPDATE caixa SET 
                saldo_atual = saldo_atual + ?,
                ultima_atualizacao = ?
            ''', (valor, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        else:
            cursor.execute('''
            UPDATE caixa SET 
                saldo_atual = saldo_atual - ?,
                ultima_atualizacao = ?
            ''', (valor, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
        conn.commit()
        return movimento_id
    except Exception as e:
        conn.rollback()
        print(f"Erro ao registrar movimento de caixa: {str(e)}")
        raise e
    finally:
        conn.close()

def abrir_caixa(valor_inicial):
    """Abre o caixa com um valor inicial"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Obter data/hora atual no formato correto
    data_atual = get_datetime_now()
    
    # Verificar se já existe caixa
    cursor.execute("SELECT * FROM caixa")
    caixa_existente = cursor.fetchone()
    
    if caixa_existente:
        # Atualizar caixa existente
        cursor.execute('''
        UPDATE caixa SET 
            saldo_inicial = ?,
            saldo_atual = ?,
            ultima_atualizacao = ?
        WHERE id = ?
        ''', (valor_inicial, valor_inicial, data_atual, caixa_existente['id']))
    else:
        # Criar novo caixa
        cursor.execute('''
        INSERT INTO caixa 
        (saldo_inicial, saldo_atual, ultima_atualizacao)
        VALUES (?, ?, ?)
        ''', (valor_inicial, valor_inicial, data_atual))
    
    # Limpar movimentações anteriores e registrar a abertura
    cursor.execute("DELETE FROM movimentos_caixa")
    
    # Registrar movimento explícito de abertura
    cursor.execute('''
    INSERT INTO movimentos_caixa 
    (data, tipo, descricao, valor)
    VALUES (?, ?, ?, ?)
    ''', (
        data_atual,
        "entrada",
        "Abertura de caixa",
        valor_inicial
    ))
    
    conn.commit()
    conn.close()

    return True

def fechar_caixa(usuario=None, valor_final=None):
    """Fecha o caixa atual e registra o valor final"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Buscar dados do caixa atual
        cursor.execute('SELECT * FROM caixa WHERE id = 1')
        caixa = dict(cursor.fetchone())
        
        # Se valor_final não foi fornecido, usar o saldo atual
        if valor_final is None:
            valor_final = caixa["saldo_atual"]
            
        # Se usuario não foi fornecido, usar um padrão
        if usuario is None:
            usuario = "sistema"
        
        # Registrar movimento de fechamento
        diferenca = valor_final - caixa["saldo_atual"]
        descricao = f"Fechamento de caixa - Operador: {usuario}"
        data_atual = get_datetime_now()
        
        # Registrar o fechamento do caixa
        cursor.execute('''
        INSERT INTO movimentos_caixa (data, tipo, descricao, valor)
        VALUES (?, ?, ?, ?)
        ''', (
            data_atual,
            "fechamento",
            descricao,
            valor_final
        ))
        
        # Salvar saldo atual para retornar um resumo
        saldo_inicial = caixa["saldo_inicial"]
        saldo_final = caixa["saldo_atual"]
        
        # Buscar movimentos do dia para o resumo
        cursor.execute('''
        SELECT tipo, SUM(valor) as total 
        FROM movimentos_caixa 
        WHERE data >= ? AND tipo != 'fechamento'
        GROUP BY tipo
        ''', (caixa["ultima_atualizacao"],))
        
        movimentos = cursor.fetchall()
        total_entradas = 0
        total_saidas = 0
        
        for mov in movimentos:
            if mov['tipo'].lower() in ['entrada', 'venda']:
                total_entradas += mov['total']
            else:
                total_saidas += mov['total']
        
        # Atualizar caixa - zerando completamente o saldo
        cursor.execute('''
        UPDATE caixa SET 
            saldo_inicial = 0,
            saldo_atual = 0,
            ultima_atualizacao = ?
        WHERE id = 1
        ''', (data_atual,))
        
        conn.commit()
        
        # Preparar resumo para retornar
        resumo = {
            'data_fechamento': data_atual,
            'saldo_inicial': saldo_inicial,
            'total_entradas': total_entradas,
            'total_saidas': total_saidas,
            'saldo_final': saldo_final
        }
        
        return resumo if usuario == "sistema" else (True, "Caixa fechado com sucesso")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao fechar caixa: {str(e)}")
        return False, f"Erro ao fechar caixa: {str(e)}"
    finally:
        conn.close()

def get_usuarios():
    """Retorna a lista de usuários"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM usuarios")
    usuarios = [dict(user) for user in cursor.fetchall()]
    
    conn.close()
    return usuarios

def verificar_login(usuario, senha):
    """Verifica as credenciais de login"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se o usuário existe
        cursor.execute("SELECT * FROM usuarios WHERE usuario = ?", (usuario,))
        user = cursor.fetchone()
        
        if not user:
            # Usuário não encontrado
            return None
        
        # Verificar a senha (comparando com a senha em texto plano para o admin)
        if user['usuario'] == 'admin' and senha == 'admin':
            return dict(user)
        
        # Para outros usuários, verificar a senha hash
        senha_hash = hash_password(senha)
        if user['senha'] == senha_hash:
            return dict(user)
        
        return None
    finally:
        conn.close()

def cadastrar_usuario(nome, usuario, senha, tipo="vendedor"):
    """Cadastra um novo usuário"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar se o usuário já existe
    cursor.execute("SELECT * FROM usuarios WHERE usuario = ?", (usuario,))
    if cursor.fetchone():
        conn.close()
        return False, "Nome de usuário já existe. Por favor escolha outro."
    
    # Verificar se o nome de usuário tem formato válido
    if not usuario.isalnum():
        conn.close()
        return False, "Nome de usuário deve conter apenas letras e números."
    
    if len(usuario) < 3:
        conn.close()
        return False, "Nome de usuário deve ter pelo menos 3 caracteres."
    
    # Verificar requisitos de senha
    if len(senha) < 4:
        conn.close()
        return False, "Senha deve ter pelo menos 4 caracteres."
    
    senha_hash = hash_password(senha)
    
    # Inserir usuário
    cursor.execute('''
    INSERT INTO usuarios 
    (nome, usuario, senha, tipo)
    VALUES (?, ?, ?, ?)
    ''', (nome, usuario, senha_hash, tipo))
    
    conn.commit()
    conn.close()
    
    return True, "Usuário cadastrado com sucesso"

def atualizar_estoque(codigo_produto, quantidade_delta):
    """Atualiza o estoque de um produto pelo código
    
    Args:
        codigo_produto: Código do produto a atualizar
        quantidade_delta: Quantidade a adicionar (positivo) ou remover (negativo)
    
    Returns:
        bool: True se o produto foi encontrado e atualizado, False caso contrário
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    UPDATE produtos SET quantidade = CASE 
        WHEN quantidade + ? < 0 THEN 0 
        ELSE quantidade + ? 
    END
    WHERE codigo = ?
    ''', (quantidade_delta, quantidade_delta, codigo_produto))
    
    if cursor.rowcount > 0:
        conn.commit()
        conn.close()
        return True
    
    conn.close()
    return False 

def get_itens_venda(venda_id):
    """Retorna os itens de uma venda específica"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        SELECT * FROM itens_venda WHERE venda_id = ?
        ''', (venda_id,))
        
        itens = [dict(item) for item in cursor.fetchall()]
        return itens
    except Exception as e:
        print(f"Erro ao buscar itens da venda {venda_id}: {str(e)}")
        return []
    finally:
        conn.close()

def get_produtos_mais_vendidos(data_inicio=None, data_fim=None):
    """Retorna os produtos mais vendidos em um determinado período"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verificar se a coluna data_venda existe
    cursor.execute("PRAGMA table_info(vendas)")
    colunas = cursor.fetchall()
    tem_data_venda = any(coluna[1] == 'data_venda' for coluna in colunas)
    coluna_data = 'data_venda' if tem_data_venda else 'data'
    
    try:
        # Verificar se a tabela itens_venda existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='itens_venda'")
        if not cursor.fetchone():
            print("Tabela itens_venda não encontrada")
            return []
            
        # Verificar se a tabela vendas existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'")
        if not cursor.fetchone():
            print("Tabela vendas não encontrada")
            return []
            
        query = """
        SELECT 
            iv.produto_id,
            p.nome,
            p.categoria,
            SUM(iv.quantidade) as quantidade_total,
            SUM(iv.subtotal) as valor_total
        FROM 
            itens_venda iv
        JOIN 
            vendas v ON iv.venda_id = v.id
        LEFT JOIN
            produtos p ON iv.produto_id = p.id
        """
        
        params = []
        where_clause = []
        
        if data_inicio:
            where_clause.append(f"v.{coluna_data} >= ?")
            params.append(data_inicio)
        
        if data_fim:
            where_clause.append(f"v.{coluna_data} <= ?")
            # Garantir que inclua todo o último dia
            if not ' ' in data_fim:
                data_fim = data_fim + " 23:59:59"
            params.append(data_fim)
        
        if where_clause:
            query += " WHERE " + " AND ".join(where_clause)
        
        query += """
        GROUP BY 
            iv.produto_id
        ORDER BY 
            valor_total DESC
        """
        
        cursor.execute(query, params)
        produtos = [dict(row) for row in cursor.fetchall()]
        
        return produtos
    except Exception as e:
        print(f"Erro ao buscar produtos mais vendidos: {str(e)}")
        return []
    finally:
        conn.close()

def get_images_dir():
    """Retorna o diretório de imagens da aplicação"""
    app_data = get_app_data_dir()
    images_dir = os.path.join(app_data, 'images')
    
    # Criar diretório se não existir
    os.makedirs(images_dir, exist_ok=True)
    return images_dir

def save_product_image(original_path, product_code):
    """Salva a imagem do produto no diretório apropriado
    
    Args:
        original_path: Caminho da imagem original
        product_code: Código do produto
        
    Returns:
        str: Nome do arquivo da imagem salva
    """
    if not original_path or not os.path.exists(original_path):
        return ""
        
    # Obter diretório de imagens
    images_dir = get_images_dir()
    
    # Criar nome do arquivo baseado no código do produto
    _, ext = os.path.splitext(original_path)
    image_filename = f"{product_code}{ext}"
    
    # Caminho completo para o arquivo de destino
    dest_path = os.path.join(images_dir, image_filename)
    
    try:
        # Copiar a imagem para o diretório de destino
        import shutil
        shutil.copy2(original_path, dest_path)
        print(f"Imagem copiada com sucesso para: {dest_path}")
        return image_filename
    except Exception as e:
        print(f"Erro ao copiar imagem: {str(e)}")
        return ""

def get_product_image_path(image_filename):
    """Retorna o caminho completo para a imagem do produto
    
    Args:
        image_filename: Nome do arquivo da imagem
        
    Returns:
        str: Caminho completo da imagem
    """
    if not image_filename:
        return ""
        
    images_dir = get_images_dir()
    return os.path.join(images_dir, image_filename)

def limpar_todos_produtos():
    """Remove todos os produtos do banco de dados"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se existem itens_venda referenciando produtos
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='itens_venda'")
        if cursor.fetchone():
            # Remover referências na tabela itens_venda primeiro
            cursor.execute("DELETE FROM itens_venda")
        
        # Apagar todos os produtos
        cursor.execute("DELETE FROM produtos")
        
        conn.commit()
        return True, "Todos os produtos foram removidos com sucesso!"
    except Exception as e:
        conn.rollback()
        print(f"Erro ao remover produtos: {str(e)}")
        return False, f"Erro ao remover produtos: {str(e)}"
    finally:
        conn.close() 

def limpar_todas_vendas():
    """Remove todos os registros de vendas do banco de dados"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se a tabela itens_venda existe e limpar
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='itens_venda'")
        if cursor.fetchone():
            cursor.execute("DELETE FROM itens_venda")
        
        # Limpar tabela de vendas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'")
        if cursor.fetchone():
            cursor.execute("DELETE FROM vendas")
            
        # Limpar movimentos de caixa relacionados a vendas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='movimentos_caixa'")
        if cursor.fetchone():
            cursor.execute("DELETE FROM movimentos_caixa WHERE tipo = 'entrada' AND descricao LIKE '%Venda%'")
        
        conn.commit()
        return True, "Todos os registros de vendas foram removidos com sucesso!"
    except Exception as e:
        conn.rollback()
        print(f"Erro ao remover vendas: {str(e)}")
        return False, f"Erro ao remover vendas: {str(e)}"
    finally:
        conn.close()

def limpar_fluxo_caixa():
    """Remove todos os registros de movimento de caixa e reinicia o saldo"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar se a tabela movimentos_caixa existe e limpar completamente
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='movimentos_caixa'")
        if cursor.fetchone():
            cursor.execute("DELETE FROM movimentos_caixa")
        
        # Verificar se a tabela caixa existe e reiniciar o saldo para zero
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='caixa'")
        if cursor.fetchone():
            cursor.execute("""
                UPDATE caixa SET 
                saldo_inicial = 0,
                saldo_atual = 0,
                ultima_atualizacao = ?
            """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
        
        conn.commit()
        return True, "Fluxo de caixa limpo com sucesso!"
    except Exception as e:
        conn.rollback()
        print(f"Erro ao limpar fluxo de caixa: {str(e)}")
        return False, f"Erro ao limpar fluxo de caixa: {str(e)}"
    finally:
        conn.close() 