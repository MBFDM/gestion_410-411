import streamlit as st
import pandas as pd
import re
import sqlite3
import hashlib
import datetime
from io import BytesIO
import base64

# Imports avec fallback
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    st.warning("⚠️ Plotly non disponible. Les graphiques seront désactivés. Installez avec: pip install plotly")

try:
    from streamlit_option_menu import option_menu
    MENU_AVAILABLE = True
except ImportError:
    MENU_AVAILABLE = False

# ==================== CONFIGURATION PAGE ====================
st.set_page_config(
    page_title="Gestion CP_410 & CP_411",
    page_icon="logo_1.jpg",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== LOGO ET STYLE ====================
# CSS personnalisé
st.markdown("""
<style>
    /* Style du logo dans la sidebar */
    .logo-container {
        text-align: center;
        padding: 20px 0;
        background: linear-gradient(135deg, #1E3A5F 0%, #2C5F8A 100%);
        border-radius: 10px;
        margin-bottom: 20px;
    }
    /* Cartes de métriques */
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        border-left: 4px solid #4FC3F7;
        margin: 10px 0;
    }
    .metric-card h3 {
        color: #1E3A5F;
        margin: 0;
        font-size: 14px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-card .value {
        font-size: 28px;
        font-weight: bold;
        color: #1E3A5F;
        margin: 5px 0;
    }
    .metric-card .change {
        font-size: 14px;
        color: #4CAF50;
    }
    .metric-card .change.negative {
        color: #f44336;
    }
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .main-header {
        background: linear-gradient(135deg, #1E3A5F 0%, #2C5F8A 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        margin-bottom: 20px;
    }
    .main-header h1 {
        margin: 0;
        font-size: 28px;
    }
    .main-header p {
        margin: 5px 0 0 0;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)

# ==================== DATABASE MANAGER ====================
class DatabaseManager:
    def __init__(self, db_file="gestion_410.db"):
        self.db_file = db_file
        self.init_database()
    
    def init_database(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                full_name TEXT,
                role TEXT DEFAULT 'user',
                status TEXT DEFAULT 'active',
                created_at TEXT,
                last_login TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS login_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                login_time TEXT,
                ip_address TEXT,
                status TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                action TEXT,
                details TEXT,
                timestamp TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                filename TEXT,
                file_type TEXT,
                rows_imported INTEGER,
                upload_date TEXT
            )
        ''')
        
        cursor.execute("SELECT * FROM users WHERE username = 'admin'")
        if not cursor.fetchone():
            admin_password = hashlib.sha256("admin123".encode()).hexdigest()
            cursor.execute('''
                INSERT INTO users (username, password, full_name, role, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('admin', admin_password, 'Administrateur', 'admin', 'active', datetime.datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def authenticate_user(self, username, password):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute('''
            SELECT id, username, full_name, role, status FROM users 
            WHERE username = ? AND password = ? AND status = 'active'
        ''', (username, hashed_password))
        user = cursor.fetchone()
        conn.close()
        return user
    
    def update_last_login(self, username):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET last_login = ? WHERE username = ?', 
                       (datetime.datetime.now().isoformat(), username))
        conn.commit()
        conn.close()
    
    def log_activity(self, username, action, details=""):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO activity_logs (username, action, details, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (username, action, details, datetime.datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    def log_login_attempt(self, username, status, ip="streamlit"):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO login_history (username, login_time, ip_address, status)
            VALUES (?, ?, ?, ?)
        ''', (username, datetime.datetime.now().isoformat(), ip, status))
        conn.commit()
        conn.close()
    
    def log_file_upload(self, username, filename, file_type, rows):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO file_history (username, filename, file_type, rows_imported, upload_date)
            VALUES (?, ?, ?, ?, ?)
        ''', (username, filename, file_type, rows, datetime.datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    def get_all_users(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, username, full_name, role, status, created_at, last_login 
            FROM users ORDER BY id
        ''')
        users = cursor.fetchall()
        conn.close()
        return users
    
    def get_user_count(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE status = 'active'")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_activity_stats(self, days=7):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat()
        cursor.execute("SELECT COUNT(*) FROM activity_logs WHERE timestamp > ?", (cutoff,))
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def add_user(self, username, password, full_name, role='user'):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        try:
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            cursor.execute('''
                INSERT INTO users (username, password, full_name, role, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (username, hashed_password, full_name, role, 'active', datetime.datetime.now().isoformat()))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()
    
    def update_user(self, user_id, full_name, role, status):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET full_name = ?, role = ?, status = ? WHERE id = ?',
                       (full_name, role, status, user_id))
        conn.commit()
        conn.close()
    
    def delete_user(self, user_id):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE id = ? AND username != 'admin'", (user_id,))
        conn.commit()
        conn.close()
    
    def change_password(self, username, old_password, new_password):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        old_hash = hashlib.sha256(old_password.encode()).hexdigest()
        cursor.execute("SELECT id FROM users WHERE username = ? AND password = ?", (username, old_hash))
        if cursor.fetchone():
            new_hash = hashlib.sha256(new_password.encode()).hexdigest()
            cursor.execute("UPDATE users SET password = ? WHERE username = ?", (new_hash, username))
            conn.commit()
            conn.close()
            return True
        conn.close()
        return False

# ==================== INITIALISATION SESSION ====================
def init_session_state():
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'current_user' not in st.session_state:
        st.session_state.current_user = None
    if 'cp410_data' not in st.session_state:
        st.session_state.cp410_data = None
    if 'cp411_data' not in st.session_state:
        st.session_state.cp411_data = None
    if 'numero_recu_list' not in st.session_state:
        st.session_state.numero_recu_list = []
    if 'police_associee_dict' not in st.session_state:
        st.session_state.police_associee_dict = {}
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    if 'show_all_410' not in st.session_state:
        st.session_state.show_all_410 = False
    if 'show_all_411' not in st.session_state:
        st.session_state.show_all_411 = False
    if 'verif_410_411' not in st.session_state:
        st.session_state.verif_410_411 = None
    if 'verif_411_410' not in st.session_state:
        st.session_state.verif_411_410 = None
    if 'show_change_pwd' not in st.session_state:
        st.session_state.show_change_pwd = False
    if 'show_user_mgmt' not in st.session_state:
        st.session_state.show_user_mgmt = False
    if 'verif_410_411_stats' not in st.session_state:
        st.session_state.verif_410_411_stats = None
    if 'verif_411_410_stats' not in st.session_state:
        st.session_state.verif_411_410_stats = None

# ==================== FONCTIONS AVEC CACHE ====================
@st.cache_data
def load_file(uploaded_file):
    """Charge un fichier CSV ou Excel avec cache"""
    if uploaded_file is None:
        return None
    try:
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str)
        else:
            df = pd.read_excel(uploaded_file, dtype=str)
        if df.empty:
            return None
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Erreur d'importation : {str(e)}")
        return None

@st.cache_data
def compute_policy_comparison_410_411(df410, df411):
    """Compare les polices 410 -> 411 avec cache"""
    if df410 is None or df411 is None:
        return None, None, None
    if "No Police" not in df410.columns or "No Police" not in df411.columns:
        return None, None, None
    
    cp410_policies = set(df410["No Police"].dropna().astype(str).str.strip())
    cp411_policies = set(df411["No Police"].dropna().astype(str).str.strip())
    
    common = sorted(cp410_policies.intersection(cp411_policies))
    only_410 = sorted(cp410_policies - cp411_policies)
    
    max_len = max(len(common), len(only_410))
    common_series = pd.Series(common + [""] * (max_len - len(common)))
    only_series = pd.Series(only_410 + [""] * (max_len - len(only_410)))
    
    df_result = pd.DataFrame({
        "Police_410_411": common_series,
        "Etat_1": ["Police retrouvée dans 411"] * len(common) + [""] * (max_len - len(common)),
        "Police_410_Only": only_series,
        "Etat_2": ["Police non retrouvée dans 411"] * len(only_410) + [""] * (max_len - len(only_410))
    })
    
    stats = {
        "total_410": len(cp410_policies),
        "total_411": len(cp411_policies),
        "matches": len(common),
        "differences": len(only_410),
        "match_rate": round(len(common) / len(cp410_policies) * 100, 1) if len(cp410_policies) > 0 else 0
    }
    return df_result, stats, f"Correspondances: {len(common)}/{len(cp410_policies)} ({stats['match_rate']}%)"

@st.cache_data
def compute_policy_comparison_411_410(df411, df410):
    """Compare les polices 411 -> 410 avec cache"""
    if df411 is None or df410 is None:
        return None, None, None
    if "No Police" not in df411.columns or "No Police" not in df410.columns:
        return None, None, None
    
    cp411_policies = set(df411["No Police"].dropna().astype(str).str.strip())
    cp410_policies = set(df410["No Police"].dropna().astype(str).str.strip())
    
    common = sorted(cp411_policies.intersection(cp410_policies))
    only_411 = sorted(cp411_policies - cp410_policies)
    
    max_len = max(len(common), len(only_411))
    common_series = pd.Series(common + [""] * (max_len - len(common)))
    only_series = pd.Series(only_411 + [""] * (max_len - len(only_411)))
    
    df_result = pd.DataFrame({
        "Police_411_410": common_series,
        "Etat_1": ["Police retrouvée dans 410"] * len(common) + [""] * (max_len - len(common)),
        "Police_411_Only": only_series,
        "Etat_2": ["Police non retrouvée dans 410"] * len(only_411) + [""] * (max_len - len(only_411))
    })
    
    stats = {
        "total_411": len(cp411_policies),
        "total_410": len(cp410_policies),
        "matches": len(common),
        "differences": len(only_411),
        "match_rate": round(len(common) / len(cp411_policies) * 100, 1) if len(cp411_policies) > 0 else 0
    }
    return df_result, stats, f"Correspondances: {len(common)}/{len(cp411_policies)} ({stats['match_rate']}%)"

@st.cache_data
def compute_invalid_refs(df411):
    """Trouve les références invalides dans CP_411"""
    if df411 is None:
        return None
    if "Réf Pièce" not in df411.columns:
        return None
    pattern = r"^\w+-\d+(?:/\d+)?$"
    invalid_refs = []
    for ref in df411["Réf Pièce"]:
        if pd.notna(ref) and not re.match(pattern, str(ref)):
            invalid_refs.append(str(ref))
    return invalid_refs

@st.cache_data
def compute_polices_for_recus(df411, recu_list):
    """Trouve les polices associées aux numéros de reçus"""
    if df411 is None or not recu_list:
        return {}
    if "Libellé" not in df411.columns or "No Police" not in df411.columns:
        return {}
    
    police_dict = {num: [] for num in recu_list}
    for _, row in df411.iterrows():
        libelle = str(row["Libellé"])
        police = str(row["No Police"])
        for num in recu_list:
            if num in libelle:
                police_dict[num].append(police)
    return police_dict

@st.cache_data
def get_data_stats(df):
    """Calcule des statistiques sur les données"""
    if df is None or df.empty:
        return None
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "nulls": df.isnull().sum().sum(),
        "duplicates": df.duplicated().sum()
    }

# ==================== AFFICHAGE DATAFRAME AVEC PAGINATION ====================
def display_dataframe(df, title="", key_suffix=""):
    """Affiche un DataFrame avec option d'affichage complet ou limité"""
    if df is None or df.empty:
        st.info("Aucune donnée à afficher")
        return
    
    st.subheader(title)
    total_rows = len(df)
    st.caption(f"Total : {total_rows} lignes")
    
    show_all_key = f"show_all_{key_suffix}"
    if show_all_key not in st.session_state:
        st.session_state[show_all_key] = False
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button(f"📊 Afficher tout" if not st.session_state[show_all_key] else "📄 Aperçu", key=f"toggle_{key_suffix}"):
            st.session_state[show_all_key] = not st.session_state[show_all_key]
            st.rerun()
    
    if st.session_state[show_all_key]:
        st.dataframe(df, use_container_width=True, height=500)
    else:
        st.dataframe(df.head(1000), use_container_width=True, height=400)
        if total_rows > 1000:
            st.info(f"Affichage des 1000 premières lignes seulement. Cliquez sur 'Afficher tout' pour voir l'intégralité ({total_rows} lignes).")

# ==================== METRIQUES ====================
def display_metrics(stats):
    """Affiche les métriques dans des cartes"""
    if stats:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>📄 Total CP_410</h3>
                <div class="value">{stats.get('total_410', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>📄 Total CP_411</h3>
                <div class="value">{stats.get('total_411', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card" style="border-left-color: #4CAF50;">
                <h3>✅ Correspondances</h3>
                <div class="value">{stats.get('matches', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card" style="border-left-color: #FF9800;">
                <h3>⚠️ Différences</h3>
                <div class="value">{stats.get('differences', 0)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            rate = stats.get('match_rate', 0)
            color = "#4CAF50" if rate >= 80 else "#FF9800" if rate >= 60 else "#f44336"
            st.markdown(f"""
            <div class="metric-card" style="border-left-color: {color};">
                <h3>📊 Taux de correspondance</h3>
                <div class="value">{rate}%</div>
            </div>
            """, unsafe_allow_html=True)

def display_dashboard_metrics():
    """Affiche le tableau de bord avec les métriques globales"""
    db = st.session_state.db
    
    st.markdown("### 📊 Tableau de bord")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        user_count = db.get_user_count()
        st.metric("👥 Utilisateurs actifs", user_count, delta=None)
    
    with col2:
        activity = db.get_activity_stats(7)
        st.metric("📈 Activité (7j)", activity, delta=None)
    
    with col3:
        if st.session_state.cp410_data is not None:
            rows = len(st.session_state.cp410_data)
            st.metric("📄 CP_410 lignes", rows, delta=None)
        else:
            st.metric("📄 CP_410", "Non importé", delta=None)
    
    with col4:
        if st.session_state.cp411_data is not None:
            rows = len(st.session_state.cp411_data)
            st.metric("📄 CP_411 lignes", rows, delta=None)
        else:
            st.metric("📄 CP_411", "Non importé", delta=None)

# ==================== GRAPHIQUES ====================
def create_comparison_chart(stats):
    """Crée un graphique de comparaison"""
    if not PLOTLY_AVAILABLE or not stats:
        return None
    
    fig = go.Figure(data=[
        go.Bar(name='CP_410', x=['Polices'], y=[stats.get('total_410', 0)], 
               marker_color='#1E3A5F', text=[stats.get('total_410', 0)], textposition='auto'),
        go.Bar(name='CP_411', x=['Polices'], y=[stats.get('total_411', 0)], 
               marker_color='#4FC3F7', text=[stats.get('total_411', 0)], textposition='auto'),
        go.Bar(name='Correspondances', x=['Polices'], y=[stats.get('matches', 0)], 
               marker_color='#4CAF50', text=[stats.get('matches', 0)], textposition='auto')
    ])
    
    fig.update_layout(
        barmode='group',
        title='Comparaison des polices',
        xaxis_title='Catégorie',
        yaxis_title='Nombre',
        showlegend=True,
        height=300,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

def create_pie_chart(stats):
    """Crée un graphique en camembert"""
    if not PLOTLY_AVAILABLE or not stats:
        return None
    
    matches = stats.get('matches', 0)
    differences = stats.get('differences', 0)
    
    fig = go.Figure(data=[go.Pie(
        labels=['Correspondances', 'Différences'],
        values=[matches, differences],
        marker_colors=['#4CAF50', '#f44336'],
        hole=0.3,
        textinfo='label+percent',
        textposition='auto'
    )])
    
    fig.update_layout(
        title='Taux de correspondance',
        height=300,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

# ==================== LOGIN / LOGOUT ====================
def login_page():
    """Page de connexion avec logo"""
    st.markdown("### 🔐 Connexion sécurisée")
    
    with st.form("login_form"):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            username = st.text_input("👤 Nom d'utilisateur", placeholder="Entrez votre nom d'utilisateur")
            password = st.text_input("🔑 Mot de passe", type="password", placeholder="Entrez votre mot de passe")
            submitted = st.form_submit_button("🚀 Se connecter", use_container_width=True)
            
            if submitted:
                if not username or not password:
                    st.error("Veuillez saisir votre nom d'utilisateur et mot de passe")
                else:
                    user = st.session_state.db.authenticate_user(username, password)
                    if user:
                        st.session_state.db.update_last_login(username)
                        st.session_state.db.log_login_attempt(username, "success")
                        st.session_state.db.log_activity(username, "Connexion", "Utilisateur connecté avec succès")
                        st.session_state.authenticated = True
                        st.session_state.current_user = user
                        st.rerun()
                    else:
                        st.session_state.db.log_login_attempt(username, "failed")
                        st.error("❌ Nom d'utilisateur ou mot de passe incorrect")

def logout():
    if st.session_state.current_user:
        st.session_state.db.log_activity(st.session_state.current_user[1], "Déconnexion", "Utilisateur déconnecté")
    st.session_state.authenticated = False
    st.session_state.current_user = None
    st.session_state.cp410_data = None
    st.session_state.cp411_data = None
    st.rerun()

# ==================== USER MANAGEMENT ====================
def user_management_section():
    st.subheader("👥 Gestion des Utilisateurs")
    db = st.session_state.db
    users = db.get_all_users()
    
    with st.expander("➕ Ajouter un utilisateur", expanded=False):
        with st.form("add_user_form"):
            col1, col2 = st.columns(2)
            with col1:
                new_username = st.text_input("👤 Nom d'utilisateur*")
                new_password = st.text_input("🔑 Mot de passe*", type="password")
                new_confirm = st.text_input("✅ Confirmer le mot de passe*", type="password")
            with col2:
                new_fullname = st.text_input("📝 Nom complet")
                new_role = st.selectbox("🎯 Rôle", ["user", "admin"])
            
            submitted = st.form_submit_button("➕ Ajouter", use_container_width=True)
            if submitted:
                if not new_username or not new_password:
                    st.error("Veuillez remplir tous les champs obligatoires")
                elif new_password != new_confirm:
                    st.error("Les mots de passe ne correspondent pas")
                elif len(new_password) < 6:
                    st.error("Le mot de passe doit contenir au moins 6 caractères")
                else:
                    if db.add_user(new_username, new_password, new_fullname, new_role):
                        st.success(f"✅ Utilisateur {new_username} ajouté avec succès")
                        st.rerun()
                    else:
                        st.error("❌ Ce nom d'utilisateur existe déjà")
    
    if users:
        user_df = pd.DataFrame(users, columns=["ID", "Utilisateur", "Nom complet", "Rôle", "Statut", "Créé le", "Dernière connexion"])
        st.dataframe(user_df, use_container_width=True, hide_index=True)
        
        for user in users:
            user_id, username, full_name, role, status, _, _ = user
            if username == "admin" and st.session_state.current_user[3] != "admin":
                continue
            with st.expander(f"✏️ Modifier / Supprimer - {username}"):
                col1, col2 = st.columns(2)
                with col1:
                    new_fullname = st.text_input("Nom complet", value=full_name or "", key=f"fullname_{user_id}")
                    new_role = st.selectbox("Rôle", ["user", "admin"], index=0 if role=="user" else 1, key=f"role_{user_id}")
                    new_status = st.selectbox("Statut", ["active", "inactive"], index=0 if status=="active" else 1, key=f"status_{user_id}")
                    if st.button("💾 Enregistrer", key=f"save_{user_id}"):
                        db.update_user(user_id, new_fullname, new_role, new_status)
                        st.success("✅ Utilisateur modifié")
                        st.rerun()
                with col2:
                    if st.button("🗑️ Supprimer", key=f"del_{user_id}"):
                        if st.checkbox("Confirmer la suppression", key=f"confirm_{user_id}"):
                            db.delete_user(user_id)
                            st.success("✅ Utilisateur supprimé")
                            st.rerun()
    else:
        st.info("Aucun utilisateur trouvé")

def change_password_section():
    st.subheader("🔑 Changer mon mot de passe")
    with st.form("change_pwd_form"):
        col1, col2 = st.columns(2)
        with col1:
            old_pwd = st.text_input("🔐 Mot de passe actuel", type="password")
            new_pwd = st.text_input("🆕 Nouveau mot de passe", type="password")
        with col2:
            confirm_pwd = st.text_input("✅ Confirmer le nouveau mot de passe", type="password")
        
        submitted = st.form_submit_button("🔄 Changer le mot de passe", use_container_width=True)
        if submitted:
            if not old_pwd or not new_pwd:
                st.error("Veuillez remplir tous les champs")
            elif new_pwd != confirm_pwd:
                st.error("Les nouveaux mots de passe ne correspondent pas")
            elif len(new_pwd) < 6:
                st.error("Le mot de passe doit contenir au moins 6 caractères")
            else:
                if st.session_state.db.change_password(st.session_state.current_user[1], old_pwd, new_pwd):
                    st.success("✅ Mot de passe modifié avec succès")
                else:
                    st.error("❌ Mot de passe actuel incorrect")

# ==================== FONCTIONS D'IMPORT ====================
def import_file_section(data_type):
    """Section d'import avec gestion d'état et logging"""
    uploaded_file = st.file_uploader(f"📤 Importer {data_type}", type=["csv", "xlsx", "xls"], key=f"upload_{data_type}")
    if uploaded_file is not None:
        with st.spinner(f"⏳ Chargement de {data_type} en cours..."):
            df = load_file(uploaded_file)
            if df is not None:
                st.success(f"✅ Données importées : {len(df)} lignes")
                if st.session_state.current_user:
                    st.session_state.db.log_file_upload(
                        st.session_state.current_user[1],
                        uploaded_file.name,
                        data_type,
                        len(df)
                    )
                return df
            else:
                st.error("❌ Le fichier est vide ou corrompu")
                return None
    return None

# ==================== EXPORT ====================
def export_to_excel(df, filename):
    """Convertit un DataFrame en fichier Excel téléchargeable"""
    if df is None or df.empty:
        return None
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# ==================== INTERFACE PRINCIPALE ====================
def main_app():
    init_session_state()
    
    if not st.session_state.authenticated:
        login_page()
        return
    
    # Barre latérale
    with st.sidebar:
        # Logo
        st.markdown("""
        <div class="logo-container">
            <h2 style="color: white; margin: 0;">📊 CP_410/411</h2>
            <p style="color: #4FC3F7; margin: 5px 0 0 0; font-size: 12px;">Gestion et Rapprochement</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div style="text-align: center; padding: 10px; background: #e8f4f8; border-radius: 8px; margin-bottom: 15px;">
            <strong>👤 {st.session_state.current_user[2]}</strong><br>
            <span style="font-size: 12px; color: #666;">{st.session_state.current_user[3]}</span>
        </div>
        """, unsafe_allow_html=True)
        
        # Menu avec option_menu si disponible, sinon menu simple
        menu_items = ["🏠 Tableau de bord", "📄 CP_410", "📄 CP_411", "🔍 Vérification 410→411", "🔍 Vérification 411→410", "📋 Rapprochement"]
        
        if st.session_state.current_user[3] == "admin":
            menu_items.append("👥 Administration")
        
        if MENU_AVAILABLE:
            selected = option_menu(
                menu_title=None,
                options=menu_items,
                icons=["house", "file", "file", "search", "search", "clipboard", "gear"] if len(menu_items) > 6 else ["house", "file", "file", "search", "search", "clipboard"],
                menu_icon="cast",
                default_index=0,
                styles={
                    "container": {"padding": "0!important", "background-color": "#fafafa"},
                    "icon": {"color": "#1E3A5F", "font-size": "18px"},
                    "nav-link": {"font-size": "14px", "text-align": "left", "margin": "0px", "--hover-color": "#e8f4f8"},
                    "nav-link-selected": {"background-color": "#1E3A5F", "color": "white"},
                }
            )
        else:
            # Menu simple si option_menu n'est pas disponible
            selected = st.radio("Navigation", menu_items, index=0)
        
        st.divider()
        
        if st.button("🔑 Changer mon mot de passe", use_container_width=True):
            st.session_state.show_change_pwd = not st.session_state.get("show_change_pwd", False)
        
        if st.session_state.get("show_change_pwd", False):
            change_password_section()
        
        if st.button("🚪 Déconnexion", use_container_width=True):
            logout()
    
    # Contenu principal
    if selected == "🏠 Tableau de bord":
        st.markdown("""
        <div class="main-header">
            <h1>📊 Tableau de bord</h1>
            <p>Vue d'ensemble des données et métriques</p>
        </div>
        """, unsafe_allow_html=True)
        
        display_dashboard_metrics()
        
        st.markdown("---")
        
        if st.session_state.cp410_data is not None and st.session_state.cp411_data is not None:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 📊 Statistiques CP_410")
                stats410 = get_data_stats(st.session_state.cp410_data)
                if stats410:
                    st.json(stats410)
            
            with col2:
                st.markdown("#### 📊 Statistiques CP_411")
                stats411 = get_data_stats(st.session_state.cp411_data)
                if stats411:
                    st.json(stats411)
            
            if PLOTLY_AVAILABLE:
                st.markdown("#### 📈 Analyse comparative")
                df_result, stats, _ = compute_policy_comparison_410_411(
                    st.session_state.cp410_data, 
                    st.session_state.cp411_data
                )
                
                if stats:
                    col1, col2 = st.columns(2)
                    with col1:
                        fig1 = create_comparison_chart(stats)
                        if fig1:
                            st.plotly_chart(fig1, use_container_width=True)
                    with col2:
                        fig2 = create_pie_chart(stats)
                        if fig2:
                            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("💡 Importez les fichiers CP_410 et CP_411 pour voir les analyses détaillées")
    
    elif selected == "📄 CP_410":
        st.markdown("""
        <div class="main-header">
            <h1>📄 Gestion CP_410</h1>
            <p>Importation et visualisation des données CP_410</p>
        </div>
        """, unsafe_allow_html=True)
        
        df = import_file_section("CP_410")
        if df is not None:
            st.session_state.cp410_data = df
        
        if st.session_state.cp410_data is not None:
            stats = get_data_stats(st.session_state.cp410_data)
            if stats:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("📄 Lignes", stats["rows"])
                with col2:
                    st.metric("📊 Colonnes", stats["columns"])
                with col3:
                    st.metric("⚠️ Valeurs manquantes", stats["nulls"])
            
            display_dataframe(st.session_state.cp410_data, title="Données CP_410", key_suffix="410")
            
            excel_data = export_to_excel(st.session_state.cp410_data, "cp410_export.xlsx")
            if excel_data:
                st.download_button(
                    "📥 Exporter CP_410 vers Excel", 
                    data=excel_data, 
                    file_name="cp410_export.xlsx", 
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
    
    elif selected == "📄 CP_411":
        st.markdown("""
        <div class="main-header">
            <h1>📄 Gestion CP_411</h1>
            <p>Importation et visualisation des données CP_411</p>
        </div>
        """, unsafe_allow_html=True)
        
        df = import_file_section("CP_411")
        if df is not None:
            st.session_state.cp411_data = df
        
        if st.session_state.cp411_data is not None:
            stats = get_data_stats(st.session_state.cp411_data)
            if stats:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("📄 Lignes", stats["rows"])
                with col2:
                    st.metric("📊 Colonnes", stats["columns"])
                with col3:
                    st.metric("⚠️ Valeurs manquantes", stats["nulls"])
            
            display_dataframe(st.session_state.cp411_data, title="Données CP_411", key_suffix="411")
            
            excel_data = export_to_excel(st.session_state.cp411_data, "cp411_export.xlsx")
            if excel_data:
                st.download_button(
                    "📥 Exporter CP_411 vers Excel", 
                    data=excel_data, 
                    file_name="cp411_export.xlsx", 
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
    
    elif selected == "🔍 Vérification 410→411":
        st.markdown("""
        <div class="main-header">
            <h1>🔍 Vérification 410 → 411</h1>
            <p>Comparaison des polices CP_410 avec CP_411</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("🔍 Vérifier", use_container_width=True):
                with st.spinner("Calcul en cours..."):
                    df_result, stats, stats_text = compute_policy_comparison_410_411(
                        st.session_state.cp410_data, 
                        st.session_state.cp411_data
                    )
                    if df_result is not None:
                        st.session_state.verif_410_411 = df_result
                        st.session_state.verif_410_411_stats = stats
                        st.success(stats_text)
                    else:
                        st.warning("Veuillez importer les deux fichiers et vérifier la colonne 'No Police'")
        
        if st.session_state.get('verif_410_411_stats'):
            display_metrics(st.session_state.verif_410_411_stats)
        
        if st.session_state.verif_410_411 is not None:
            st.markdown("---")
            display_dataframe(st.session_state.verif_410_411, title="Résultats 410 → 411", key_suffix="verif410")
            excel_data = export_to_excel(st.session_state.verif_410_411, "verification_410_411.xlsx")
            if excel_data:
                st.download_button(
                    "📥 Exporter les résultats", 
                    data=excel_data, 
                    file_name="verification_410_411.xlsx",
                    use_container_width=True
                )
    
    elif selected == "🔍 Vérification 411→410":
        st.markdown("""
        <div class="main-header">
            <h1>🔍 Vérification 411 → 410</h1>
            <p>Comparaison des polices CP_411 avec CP_410</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("🔍 Vérifier", use_container_width=True):
                with st.spinner("Calcul en cours..."):
                    df_result, stats, stats_text = compute_policy_comparison_411_410(
                        st.session_state.cp411_data, 
                        st.session_state.cp410_data
                    )
                    if df_result is not None:
                        st.session_state.verif_411_410 = df_result
                        st.session_state.verif_411_410_stats = stats
                        st.success(stats_text)
                    else:
                        st.warning("Veuillez importer les deux fichiers et vérifier la colonne 'No Police'")
        
        if st.session_state.get('verif_411_410_stats'):
            display_metrics(st.session_state.verif_411_410_stats)
        
        if st.session_state.verif_411_410 is not None:
            st.markdown("---")
            display_dataframe(st.session_state.verif_411_410, title="Résultats 411 → 410", key_suffix="verif411")
            excel_data = export_to_excel(st.session_state.verif_411_410, "verification_411_410.xlsx")
            if excel_data:
                st.download_button(
                    "📥 Exporter les résultats", 
                    data=excel_data, 
                    file_name="verification_411_410.xlsx",
                    use_container_width=True
                )
    
    elif selected == "📋 Rapprochement":
        st.markdown("""
        <div class="main-header">
            <h1>📋 Rapprochement</h1>
            <p>Vérification des références et association avec les polices</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Vérifier les Réf Pièce", use_container_width=True):
                with st.spinner("Recherche des références invalides..."):
                    invalid = compute_invalid_refs(st.session_state.cp411_data)
                    if invalid is not None:
                        st.session_state.numero_recu_list = invalid
                        st.success(f"✅ Références invalides trouvées : {len(invalid)}")
                    else:
                        st.warning("Veuillez importer CP_411 avec une colonne 'Réf Pièce'")
        
        with col2:
            if st.button("🔎 Trouver les polices associées", use_container_width=True):
                if st.session_state.numero_recu_list:
                    with st.spinner("Recherche des polices associées..."):
                        police_dict = compute_polices_for_recus(
                            st.session_state.cp411_data, 
                            st.session_state.numero_recu_list
                        )
                        st.session_state.police_associee_dict = police_dict
                        
                        total_with_polices = sum(1 for p in police_dict.values() if p)
                        st.success(f"✅ {total_with_polices}/{len(police_dict)} références ont des polices associées")
                else:
                    st.warning("Veuillez d'abord lancer 'Vérifier les Réf Pièce'")
        
        if st.session_state.numero_recu_list:
            st.markdown("---")
            st.subheader(f"📄 Références invalides ({len(st.session_state.numero_recu_list)})")
            
            total_polices = sum(len(p) for p in st.session_state.police_associee_dict.values())
            with_polices = sum(1 for p in st.session_state.police_associee_dict.values() if p)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📄 Références invalides", len(st.session_state.numero_recu_list))
            with col2:
                st.metric("🔗 Avec polices associées", with_polices)
            with col3:
                st.metric("📊 Total polices trouvées", total_polices)
            
            st.markdown("---")
            
            for numero in st.session_state.numero_recu_list:
                polices = st.session_state.police_associee_dict.get(numero, [])
                with st.expander(f"📄 {numero} ({len(polices)} police(s) associée(s))"):
                    if polices:
                        for p in polices:
                            st.write(f"- {p}")
                    else:
                        st.write("Aucune police associée trouvée")
            
            export_data = []
            for num, polices in st.session_state.police_associee_dict.items():
                export_data.append({
                    "Numéro reçu": num, 
                    "Polices associées": ", ".join(polices) if polices else "Aucune"
                })
            if export_data:
                df_export = pd.DataFrame(export_data)
                excel_data = export_to_excel(df_export, "rapprochement.xlsx")
                if excel_data:
                    st.download_button(
                        "📥 Exporter le rapprochement vers Excel", 
                        data=excel_data, 
                        file_name="rapprochement.xlsx",
                        use_container_width=True
                    )
    
    elif selected == "👥 Administration":
        st.markdown("""
        <div class="main-header">
            <h1>👥 Administration</h1>
            <p>Gestion des utilisateurs et paramètres système</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.session_state.current_user[3] == "admin":
            user_management_section()
        else:
            st.error("❌ Accès non autorisé. Vous devez être administrateur.")

if __name__ == "__main__":
    main_app()
