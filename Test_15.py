import streamlit as st
import pandas as pd
import re
import sqlite3
import hashlib
import datetime
from io import BytesIO

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
        return None, None
    if "No Police" not in df410.columns or "No Police" not in df411.columns:
        return None, None
    
    cp410_policies = set(df410["No Police"].dropna().astype(str).str.strip())
    cp411_policies = set(df411["No Police"].dropna().astype(str).str.strip())
    
    common = sorted(cp410_policies.intersection(cp411_policies))
    only_410 = sorted(cp410_policies - cp411_policies)
    
    max_len = max(len(common), len(only_410))
    # Version vectorisée
    common_series = pd.Series(common + [""] * (max_len - len(common)))
    only_series = pd.Series(only_410 + [""] * (max_len - len(only_410)))
    
    df_result = pd.DataFrame({
        "Police_410_411": common_series,
        "Etat_1": ["Police retrouvée dans 411"] * len(common) + [""] * (max_len - len(common)),
        "Police_410_Only": only_series,
        "Etat_2": ["Police non retrouvée dans 411"] * len(only_410) + [""] * (max_len - len(only_410))
    })
    stats = f"Total CP_410: {len(cp410_policies)} | Total CP_411: {len(cp411_policies)} | Correspondances: {len(common)} | Différences: {len(only_410)}"
    return df_result, stats

@st.cache_data
def compute_policy_comparison_411_410(df411, df410):
    """Compare les polices 411 -> 410 avec cache"""
    if df411 is None or df410 is None:
        return None, None
    if "No Police" not in df411.columns or "No Police" not in df410.columns:
        return None, None
    
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
    stats = f"Total CP_411: {len(cp411_policies)} | Total CP_410: {len(cp410_policies)} | Correspondances: {len(common)} | Différences: {len(only_411)}"
    return df_result, stats

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

# ==================== AFFICHAGE DATAFRAME AVEC PAGINATION ====================
def display_dataframe(df, title="", key_suffix=""):
    """Affiche un DataFrame avec option d'affichage complet ou limité"""
    if df is None or df.empty:
        st.info("Aucune donnée à afficher")
        return
    
    st.subheader(title)
    total_rows = len(df)
    st.caption(f"Total : {total_rows} lignes")
    
    # Option pour afficher tout ou partie
    show_all_key = f"show_all_{key_suffix}"
    if show_all_key not in st.session_state:
        st.session_state[show_all_key] = False
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button(f"Afficher tout" if not st.session_state[show_all_key] else "Afficher un aperçu", key=f"toggle_{key_suffix}"):
            st.session_state[show_all_key] = not st.session_state[show_all_key]
            st.rerun()
    
    if st.session_state[show_all_key]:
        st.dataframe(df, use_container_width=True)
    else:
        st.dataframe(df.head(1000), use_container_width=True)
        if total_rows > 1000:
            st.info(f"Affichage des 1000 premières lignes seulement. Cliquez sur 'Afficher tout' pour voir l'intégralité ({total_rows} lignes).")

# ==================== LOGIN / LOGOUT ====================
def login_page():
    st.title("🔒 Gestion CP_410 & CP_411")
    st.markdown("### Outil de Rapprochement et Vérification")
    
    with st.form("login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        submitted = st.form_submit_button("Se connecter")
        
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
                    st.error("Nom d'utilisateur ou mot de passe incorrect")
    
    st.caption("Identifiants par défaut : admin / admin123")

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
    
    # Formulaire d'ajout
    with st.expander("➕ Ajouter un utilisateur"):
        with st.form("add_user_form"):
            new_username = st.text_input("Nom d'utilisateur")
            new_password = st.text_input("Mot de passe", type="password")
            new_confirm = st.text_input("Confirmer le mot de passe", type="password")
            new_fullname = st.text_input("Nom complet")
            new_role = st.selectbox("Rôle", ["user", "admin"])
            submitted = st.form_submit_button("Ajouter")
            if submitted:
                if not new_username or not new_password:
                    st.error("Veuillez remplir tous les champs obligatoires")
                elif new_password != new_confirm:
                    st.error("Les mots de passe ne correspondent pas")
                elif len(new_password) < 6:
                    st.error("Le mot de passe doit contenir au moins 6 caractères")
                else:
                    if db.add_user(new_username, new_password, new_fullname, new_role):
                        st.success(f"Utilisateur {new_username} ajouté avec succès")
                        st.rerun()
                    else:
                        st.error("Ce nom d'utilisateur existe déjà")
    
    # Liste des utilisateurs
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
                    if st.button("Enregistrer modifications", key=f"save_{user_id}"):
                        db.update_user(user_id, new_fullname, new_role, new_status)
                        st.success("Utilisateur modifié")
                        st.rerun()
                with col2:
                    if st.button("🗑️ Supprimer", key=f"del_{user_id}"):
                        if st.checkbox("Confirmer la suppression", key=f"confirm_{user_id}"):
                            db.delete_user(user_id)
                            st.success("Utilisateur supprimé")
                            st.rerun()
    else:
        st.info("Aucun utilisateur trouvé")

def change_password_section():
    st.subheader("🔑 Changer mon mot de passe")
    with st.form("change_pwd_form"):
        old_pwd = st.text_input("Mot de passe actuel", type="password")
        new_pwd = st.text_input("Nouveau mot de passe", type="password")
        confirm_pwd = st.text_input("Confirmer le nouveau mot de passe", type="password")
        submitted = st.form_submit_button("Changer le mot de passe")
        if submitted:
            if not old_pwd or not new_pwd:
                st.error("Veuillez remplir tous les champs")
            elif new_pwd != confirm_pwd:
                st.error("Les nouveaux mots de passe ne correspondent pas")
            elif len(new_pwd) < 6:
                st.error("Le mot de passe doit contenir au moins 6 caractères")
            else:
                if st.session_state.db.change_password(st.session_state.current_user[1], old_pwd, new_pwd):
                    st.success("Mot de passe modifié avec succès")
                else:
                    st.error("Mot de passe actuel incorrect")

# ==================== FONCTIONS D'IMPORT ====================
def import_file_section(data_type):
    """Section d'import avec gestion d'état"""
    uploaded_file = st.file_uploader(f"Importer {data_type}", type=["csv", "xlsx", "xls"], key=f"upload_{data_type}")
    if uploaded_file is not None:
        with st.spinner(f"Chargement de {data_type} en cours..."):
            df = load_file(uploaded_file)
            if df is not None:
                st.success(f"Données importées : {len(df)} lignes")
                return df
            else:
                st.error("Le fichier est vide ou corrompu")
                return None
    return None

# ==================== INTERFACE PRINCIPALE ====================
def main_app():
    st.set_page_config(page_title="Gestion CP_410 & CP_411", layout="wide")
    init_session_state()
    
    if not st.session_state.authenticated:
        login_page()
        return
    
    # Barre latérale
    with st.sidebar:
        st.markdown(f"**👤 {st.session_state.current_user[2]}** ({st.session_state.current_user[3]})")
        if st.button("🚪 Déconnexion"):
            logout()
        st.divider()
        if st.button("🔑 Changer mon mot de passe"):
            st.session_state.show_change_pwd = not st.session_state.get("show_change_pwd", False)
        if st.session_state.get("show_change_pwd", False):
            change_password_section()
        if st.session_state.current_user[3] == "admin":
            st.divider()
            st.markdown("### Administration")
            if st.button("👥 Gestion des utilisateurs"):
                st.session_state.show_user_mgmt = not st.session_state.get("show_user_mgmt", False)
            if st.session_state.get("show_user_mgmt", False):
                user_management_section()
    
    # Onglets principaux
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["CP_410", "CP_411", "Vérification 410/411", "Vérification 411/410", "Rapprochement"])
    
    # Onglet CP_410
    with tab1:
        st.header("CP_410")
        df = import_file_section("CP_410")
        if df is not None:
            st.session_state.cp410_data = df
        if st.session_state.cp410_data is not None:
            display_dataframe(st.session_state.cp410_data, title="Données CP_410", key_suffix="410")
            excel_data = export_to_excel(st.session_state.cp410_data, "cp410_export.xlsx")
            if excel_data:
                st.download_button("📥 Exporter CP_410 vers Excel", data=excel_data, file_name="cp410_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    # Onglet CP_411
    with tab2:
        st.header("CP_411")
        df = import_file_section("CP_411")
        if df is not None:
            st.session_state.cp411_data = df
        if st.session_state.cp411_data is not None:
            display_dataframe(st.session_state.cp411_data, title="Données CP_411", key_suffix="411")
            excel_data = export_to_excel(st.session_state.cp411_data, "cp411_export.xlsx")
            if excel_data:
                st.download_button("📥 Exporter CP_411 vers Excel", data=excel_data, file_name="cp411_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    # Onglet Vérification 410/411
    with tab3:
        st.header("Vérification des No Police (410 → 411)")
        if st.button("🔍 Vérifier les polices 410/411"):
            with st.spinner("Calcul en cours..."):
                df_result, stats = compute_policy_comparison_410_411(st.session_state.cp410_data, st.session_state.cp411_data)
                if df_result is not None:
                    st.session_state.verif_410_411 = df_result
                    st.success(stats)
                else:
                    st.warning("Veuillez importer les deux fichiers et vérifier la colonne 'No Police'")
        if st.session_state.verif_410_411 is not None:
            display_dataframe(st.session_state.verif_410_411, title="Résultats 410 → 411", key_suffix="verif410")
            excel_data = export_to_excel(st.session_state.verif_410_411, "verification_410_411.xlsx")
            if excel_data:
                st.download_button("📥 Exporter les résultats", data=excel_data, file_name="verification_410_411.xlsx")
    
    # Onglet Vérification 411/410
    with tab4:
        st.header("Vérification des No Police (411 → 410)")
        if st.button("🔍 Vérifier les polices 411/410"):
            with st.spinner("Calcul en cours..."):
                df_result, stats = compute_policy_comparison_411_410(st.session_state.cp411_data, st.session_state.cp410_data)
                if df_result is not None:
                    st.session_state.verif_411_410 = df_result
                    st.success(stats)
                else:
                    st.warning("Veuillez importer les deux fichiers et vérifier la colonne 'No Police'")
        if st.session_state.verif_411_410 is not None:
            display_dataframe(st.session_state.verif_411_410, title="Résultats 411 → 410", key_suffix="verif411")
            excel_data = export_to_excel(st.session_state.verif_411_410, "verification_411_410.xlsx")
            if excel_data:
                st.download_button("📥 Exporter les résultats", data=excel_data, file_name="verification_411_410.xlsx")
    
    # Onglet Rapprochement
    with tab5:
        st.header("Rapprochement - Réf Pièce invalides")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📋 Vérifier les Réf Pièce"):
                with st.spinner("Recherche des références invalides..."):
                    invalid = compute_invalid_refs(st.session_state.cp411_data)
                    if invalid is not None:
                        st.session_state.numero_recu_list = invalid
                        st.success(f"Références invalides trouvées : {len(invalid)}")
                    else:
                        st.warning("Veuillez importer CP_411 avec une colonne 'Réf Pièce'")
        with col2:
            if st.button("🔎 Trouver les polices associées"):
                if st.session_state.numero_recu_list:
                    with st.spinner("Recherche des polices associées..."):
                        police_dict = compute_polices_for_recus(st.session_state.cp411_data, st.session_state.numero_recu_list)
                        st.session_state.police_associee_dict = police_dict
                        st.success("Recherche terminée.")
                else:
                    st.warning("Veuillez d'abord lancer 'Vérifier les Réf Pièce'")
        
        if st.session_state.numero_recu_list:
            st.subheader("Références invalides et polices associées")
            for numero in st.session_state.numero_recu_list:
                polices = st.session_state.police_associee_dict.get(numero, [])
                with st.expander(f"📄 {numero} ({len(polices)} police(s) associée(s))"):
                    if polices:
                        for p in polices:
                            st.write(f"- {p}")
                    else:
                        st.write("Aucune police associée trouvée")
            
            # Export des résultats
            export_data = []
            for num, polices in st.session_state.police_associee_dict.items():
                export_data.append({"Numéro reçu": num, "Polices associées": ", ".join(polices) if polices else "Aucune"})
            if export_data:
                df_export = pd.DataFrame(export_data)
                excel_data = export_to_excel(df_export, "rapprochement.xlsx")
                if excel_data:
                    st.download_button("📥 Exporter le rapprochement vers Excel", data=excel_data, file_name="rapprochement.xlsx")

# ==================== FONCTION EXPORT ====================
def export_to_excel(df, filename):
    """Convertit un DataFrame en fichier Excel téléchargeable"""
    if df is None or df.empty:
        return None
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

if __name__ == "__main__":
    main_app()