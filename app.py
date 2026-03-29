from dotenv import load_dotenv
load_dotenv()

import os
import zipfile
import shutil
import json

from flask import (
    Flask, render_template, request,
    redirect, url_for, flash, jsonify, session
)
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
import psycopg2.extras
import requests

# GeoPandas dan pandas: lazy import untuk serverless (Vercel)
# Library ini sangat besar dan mungkin tidak tersedia di production
# fiona bisa throw OSError (bukan ImportError) jika GDAL tidak ada
try:
    import geopandas as gpd
    import pandas as pd
    HAS_GEOPANDAS = True
except Exception:
    HAS_GEOPANDAS = False
from functools import wraps

# -------------------------------------------------
# Flask setup
# -------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Di Vercel serverless, filesystem read-only kecuali /tmp
IS_VERCEL = os.getenv("VERCEL", "") == "1"
if IS_VERCEL:
    UPLOAD_FOLDER = "/tmp/tmp_uploads"
    EXTRACT_FOLDER = "/tmp/tmp_extracted"
else:
    UPLOAD_FOLDER = os.path.join(BASE_DIR, "tmp_uploads")
    EXTRACT_FOLDER = os.path.join(BASE_DIR, "tmp_extracted")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

# -------------------------------------------------
# Database Supabase (Postgres)
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("WARNING: DATABASE_URL tidak ditemukan. Pastikan sudah diset di environment variables.")

def get_db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL belum diset.")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()

    # --- Tabel layer_peta (Parameter) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS layer_peta (
            id SERIAL PRIMARY KEY,
            layer_name TEXT NOT NULL,
            file_name TEXT,
            description TEXT,
            geojson JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # --- Tabel kesesuaian_lahan (BARU) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kesesuaian_lahan (
            id SERIAL PRIMARY KEY,
            layer_name TEXT NOT NULL,
            file_name TEXT,
            description TEXT,
            geojson JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # --- Tabel informasi_tambahan (BARU) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS informasi_tambahan (
            id SERIAL PRIMARY KEY,
            layer_name TEXT NOT NULL,
            file_name TEXT,
            description TEXT,
            geojson JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # --- Tabel users ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # --- Tabel parameter_lahan ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parameter_lahan (
            id SERIAL PRIMARY KEY,
            tag_label   TEXT NOT NULL,
            icon_class  TEXT,
            title       TEXT NOT NULL,
            description TEXT NOT NULL,
            order_no    INTEGER NOT NULL DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # Seed admin default
    cur.execute("SELECT COUNT(*) FROM users;")
    count = cur.fetchone()[0]
    if count == 0:
        default_username = os.getenv("DEFAULT_ADMIN_USERNAME", "admin")
        default_password = os.getenv("DEFAULT_ADMIN_PASSWORD", "admin123")
        pwd_hash = generate_password_hash(default_password)
        cur.execute(
            """
            INSERT INTO users (username, password_hash, is_admin)
            VALUES (%s, %s, %s);
            """,
            (default_username, pwd_hash, True),
        )

    # Seed default parameter
    cur.execute("SELECT COUNT(*) FROM parameter_lahan;")
    if cur.fetchone()[0] == 0:
        params_seed = [
            ("Fisik tanah", "fa-water", "Drainase",
             "Menjelaskan kemampuan tanah mengalirkan kelebihan air. Kelas cepat, sedang, agak terhambat, hingga terhambat akan memengaruhi risiko genangan dan ketersediaan air bagi semangka.", 1),
            ("Bentuklahan", "fa-mountain", "Kemiringan Lereng",
             "Lereng landai lebih ideal untuk budidaya dan mekanisasi. Lereng curam meningkatkan risiko erosi dan biaya pengelolaan lahan.", 2),
            ("Profil tanah", "fa-layer-group", "Kedalaman Efektif Tanah",
             "Semakin dalam tanah efektif, semakin leluasa perakaran berkembang dan menyerap unsur hara serta air.", 3),
            ("Kimia tanah", "fa-flask", "pH Tanah",
             "pH menentukan ketersediaan unsur hara. Semangka umumnya optimal pada kisaran pH netral—agak masam.", 4),
            ("Bahan organik", "fa-leaf", "C-Organik",
             "Menggambarkan kandungan bahan organik tanah yang berperan dalam struktur, kapasitas tukar kation, dan ketersediaan hara.", 5),
            ("Kapasitas tukar", "fa-bolt", "KTK (Kapasitas Tukar Kation)",
             "Semakin tinggi KTK, semakin baik tanah menahan dan menyediakan unsur hara bagi tanaman.", 6),
            ("Iklim", "fa-cloud-rain", "Curah Hujan",
             "Curah hujan tahunan memengaruhi ketersediaan air dan kebutuhan irigasi. Kelas disesuaikan dengan kebutuhan air semangka.", 7),
            ("Tekstur", "fa-globe", "Tekstur Tanah",
             "Kombinasi fraksi pasir—debu—liat yang berpengaruh pada aerasi, kapasitas menahan air, dan pengolahan tanah.", 8),
            ("Batuan Permukaan", "fa-gem", "Batuan Permukaan / Singkapan",
             "Persentase batuan di permukaan dapat membatasi pengolahan tanah dan sistem perakaran, sehingga menurunkan kelas kesesuaian.", 9),
        ]
        cur.executemany(
            """
            INSERT INTO parameter_lahan (tag_label, icon_class, title, description, order_no)
            VALUES (%s, %s, %s, %s, %s);
            """,
            params_seed,
        )

    conn.commit()
    cur.close()
    conn.close()

# -------------------------------------------------
# Auth helper
# -------------------------------------------------
def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            flash("Silakan login terlebih dahulu.")
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped

def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            flash("Silakan login terlebih dahulu.")
            return redirect(url_for("login", next=request.path))
        if not session.get("is_admin"):
            flash("Akses ditolak. Anda bukan admin.")
            return redirect(url_for("index"))
        return view(*args, **kwargs)
    return wrapped

# -------------------------------------------------
# ROUTES PUBLIC
# -------------------------------------------------
@app.route("/")
def index():
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Layer peta (Parameter)
    cur.execute(
        """
        SELECT id, layer_name, description, created_at
        FROM layer_peta
        ORDER BY created_at DESC;
        """
    )
    layers = cur.fetchall()

    # Kesesuaian Lahan
    cur.execute(
        """
        SELECT id, layer_name, description, created_at
        FROM kesesuaian_lahan
        ORDER BY created_at DESC;
        """
    )
    kesesuaian = cur.fetchall()

    # Informasi Tambahan
    cur.execute(
        """
        SELECT id, layer_name, description, created_at
        FROM informasi_tambahan
        ORDER BY created_at DESC;
        """
    )
    informasi = cur.fetchall()

    # Parameter lahan
    cur.execute(
        """
        SELECT id, tag_label, icon_class, title, description, order_no
        FROM parameter_lahan
        ORDER BY order_no, id;
        """
    )
    params = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "index.html",
        layers=layers,
        kesesuaian=kesesuaian,
        informasi=informasi,
        params=params,
        user=session.get("username"),
    )


# -------------------------------------------------
# LOGIN / LOGOUT
# -------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Username dan password wajib diisi.")
            return redirect(url_for("login"))

        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT id, username, password_hash, is_admin FROM users WHERE username = %s;",
            (username,),
        )
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user or not check_password_hash(user["password_hash"], password):
            flash("Username atau password salah.")
            return redirect(url_for("login"))

        session["user_id"] = user["id"]
        session["username"] = user["username"]
        session["is_admin"] = bool(user["is_admin"])

        flash("Login berhasil.")
        next_url = request.args.get("next") or url_for("admin")
        return redirect(next_url)

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Anda telah logout.")
    return redirect(url_for("index"))

# -------------------------------------------------
# ADMIN DASHBOARD
# -------------------------------------------------
@app.route("/admin")
@admin_required
def admin():
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # layers (Parameter)
    cur.execute(
        """
        SELECT id, layer_name, description, file_name, created_at
        FROM layer_peta
        ORDER BY created_at DESC;
        """
    )
    layers = cur.fetchall()

    # Kesesuaian Lahan
    cur.execute(
        """
        SELECT id, layer_name, description, file_name, created_at
        FROM kesesuaian_lahan
        ORDER BY created_at DESC;
        """
    )
    kesesuaian = cur.fetchall()

    # Informasi Tambahan
    cur.execute(
        """
        SELECT id, layer_name, description, file_name, created_at
        FROM informasi_tambahan
        ORDER BY created_at DESC;
        """
    )
    informasi = cur.fetchall()

    # users
    cur.execute(
        """
        SELECT id, username, is_admin, created_at
        FROM users
        ORDER BY created_at ASC;
        """
    )
    users = cur.fetchall()

    # parameter lahan
    cur.execute(
        """
        SELECT id, tag_label, icon_class, title, description, order_no, created_at
        FROM parameter_lahan
        ORDER BY order_no, id;
        """
    )
    params = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "admin.html",
        layers=layers,
        kesesuaian=kesesuaian,
        informasi=informasi,
        users=users,
        params=params,
        current_user=session.get("username"),
    )

# -------------------------------------------------
# Upload Layer Functions
# -------------------------------------------------
ALLOWED_EXTENSIONS = {".zip", ".geojson", ".json"}

def allowed_file(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXTENSIONS

def process_upload_layer(layer_name, description, file, table_name):
    """Helper function untuk upload layer ke tabel yang ditentukan"""
    if not HAS_GEOPANDAS:
        return False, "Error: Library GeoPandas tidak tersedia di server ini. Upload shapefile tidak bisa dilakukan."
    if not layer_name or not file:
        return False, "Nama layer dan file wajib diisi."

    filename = secure_filename(file.filename)
    if not allowed_file(filename):
        return False, "Format file tidak didukung. Gunakan ZIP SHP atau GeoJSON."

    save_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(save_path)

    try:
        gdf = None

        # ZIP → extract → baca SHP
        if filename.lower().endswith(".zip"):
            extract_dir = os.path.join(EXTRACT_FOLDER, os.path.splitext(filename)[0])
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(save_path, "r") as z:
                z.extractall(extract_dir)

            shp_path = None
            for root, _, files in os.walk(extract_dir):
                for f in files:
                    if f.lower().endswith(".shp"):
                        shp_path = os.path.join(root, f)
                        break
                if shp_path:
                    break

            if not shp_path:
                shutil.rmtree(extract_dir, ignore_errors=True)
                return False, "File .shp tidak ditemukan di dalam zip."

            gdf = gpd.read_file(shp_path)

        # GeoJSON langsung
        else:
            gdf = gpd.read_file(save_path)

        # Pastikan CRS WGS84
        try:
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)
        except Exception:
            pass

        # Konversi ke GeoJSON string
        geojson_str = gdf.to_json()
        geojson_obj = json.loads(geojson_str)
        geojson_min = json.dumps(geojson_obj, separators=(",", ":"))
        geojson_obj = json.loads(geojson_min)

        # Simpan ke DB
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {table_name} (layer_name, file_name, description, geojson)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (layer_name, filename, description or None, psycopg2.extras.Json(geojson_obj)),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        return True, f'Layer "{layer_name}" berhasil diupload (ID: {new_id}).'

    except Exception as e:
        return False, f"Error saat memproses file: {e}"

    finally:
        if os.path.exists(save_path):
            os.remove(save_path)
        extract_dir = os.path.join(EXTRACT_FOLDER, os.path.splitext(filename)[0])
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir, ignore_errors=True)

# pandas sudah di-import di atas (lazy import bersama geopandas)

# -------------------------------------------------
# Spatial Analysis Logic (Auto)
# -------------------------------------------------
LAYER_MAP = {
    # Layer yang kita harapkan berdasarkan nama upload di DB
    "Batuan di Permukaan": "BP",
    "KTK": "KTK",
    "C Organik": "COrg",
    "pH Tanah": "pH",
    "Tekstur": "Tekstur",
    "Drainase": "Dr",
    "Salinitas": "EC",
    "Kedalaman Tanah": "KT",
    "Kemiringan Lereng": "Lereng",
    "Curah Hujan Barru": "CH",
}

def normalize_gdf_columns(gdf, param_code):
    """
    Menyeragamkan penamaan kolom dari berbagai sumber shapefile/GeoJSON.
    Mampu mendeteksi pola gaya lama 'S1_pH' atau gaya baru 'Sangat ses'.
    Mengembalikan GDF dengan skema baku: [Kode_S1, Kode_S2, Kode_S3, Kode_N, geometry] 
    dan secara eksplisit menandai Kawasan Lindung berdasarkan atribut aslinya.
    """
    actual_cols_lower = {str(c).lower().strip(): c for c in gdf.columns}
    
    class_mappings = {
        f"{param_code}_S1": [f"s1_{param_code.lower()}", "sangat ses", "sangat sesuai"],
        f"{param_code}_S2": [f"s2_{param_code.lower()}", "sesuai"],
        f"{param_code}_S3": [f"s3_{param_code.lower()}", "sesuai mar", "sesuai marginal"],
        f"{param_code}_N": [f"n_{param_code.lower()}", "tidak sesu", "tidak sesuai"]
    }
    
    cols_to_keep = ['geometry']
    raw_vals = ['ec_nilai', 'ph_nilai', 'nilai_batu', 'nilai batu', 'lereng', 'dalam_tnh', 'drainase', 'ktk_nilai', 'tekstur', 'ch_nilai', 'curah', 'tinggi']
    
    # 2. Normalisasi kolom kelas
    for std_col, variations in class_mappings.items():
        found = False
        for var in variations:
            if var in actual_cols_lower:
                original_col = actual_cols_lower[var]
                gdf[std_col] = pd.to_numeric(gdf[original_col], errors='coerce').fillna(0).astype(int)
                cols_to_keep.append(std_col)
                found = True
                break
        
        if not found:
            gdf[std_col] = 0
            cols_to_keep.append(std_col)
            
    # 3. Deteksi Kawasan Lindung (jika nilai raw/mentahnya 0 atau kosong)
    gdf['is_lindung'] = 0
    for c_lower, c_asli in actual_cols_lower.items():
        if any(r in c_lower for r in raw_vals):
            def check_lindung(val):
                val_str = str(val).strip().lower()
                if val_str in ['0', '0.0', '', 'nan', 'none', '<null>']:
                    return 1
                return 0
            gdf['is_lindung'] = gdf[c_asli].apply(check_lindung)
            break
    cols_to_keep.append('is_lindung')
            
    return gdf[list(set(cols_to_keep))]

def sanitize_geometries(gdf):
    """
    Memastikan GDF hanya berisi tipe geometri tunggal (Polygon/MultiPolygon).
    Membuang Point/LineString sisa hasil intersection dan meledakkan GeometryCollection.
    """
    if gdf is None or gdf.empty:
        return gdf
    
    valid_types = ['Polygon', 'MultiPolygon']
    gdf = gdf[gdf.geometry.geom_type.isin(valid_types)].copy()
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)
    return gdf

def overlay_layers(layers):
    if not layers: return None
    
    # Bersihkan layer pertama
    result = sanitize_geometries(layers[0])
    
    for gdf in layers[1:]:
        # Bersihkan layer penabrak
        gdf_clean = sanitize_geometries(gdf)
        
        # Kembali ke INTERSECTION karena UNION terlalu berat komputasinya untuk 10 lapis
        result = gpd.overlay(result, gdf_clean, how='intersection', keep_geom_type=True)
        
        result = sanitize_geometries(result)
        
        # Penanganan khusus is_lindung (Jika salah satu layer bilang Lindung, maka area itu total Lindung)
        if 'is_lindung_1' in result.columns and 'is_lindung_2' in result.columns:
            result['is_lindung'] = result[['is_lindung_1', 'is_lindung_2']].max(axis=1)
            result.drop(columns=['is_lindung_1', 'is_lindung_2'], inplace=True)
        elif 'is_lindung_1' in result.columns:
            result.rename(columns={'is_lindung_1': 'is_lindung'}, inplace=True)
        elif 'is_lindung_2' in result.columns:
            result.rename(columns={'is_lindung_2': 'is_lindung'}, inplace=True)
            
        # Merge kolom administratif yang sama namanya pasca-intersection 
        for col in list(result.columns):
            if str(col).endswith('_1') or str(col).endswith('_2'):
                base_col = str(col)[:-2]
                if base_col == 'is_lindung': continue
                if base_col not in result.columns:
                    result.rename(columns={col: base_col}, inplace=True)
                else:
                    # Isi data yg kosong (NaN) dengan data duplikatnya
                    result[base_col] = result[base_col].fillna(result[col])
                    result.drop(columns=[col], inplace=True)
                    
        target_cols = [c for c in result.columns if c != 'geometry']
        if target_cols:
            result = result.dissolve(by=target_cols, as_index=False)
            result = sanitize_geometries(result)
            
    return result

def tentukan_kelas(row):
    # Cek apakah terdeteksi sebagai Kawasan Lindung dari data mentah
    if row.get('is_lindung') == 1:
        return 'Kawasan Lindung / Tidak Dinilai'
        
    # Cek yang memiliki angka persis 1
    if any(row.get(col) == 1 for col in row.index if str(col).endswith('_N')):
        return 'Tidak Sesuai'
    elif any(row.get(col) == 1 for col in row.index if str(col).endswith('_S3')):
        return 'Sesuai Marginal'
    elif any(row.get(col) == 1 for col in row.index if str(col).endswith('_S2')):
        return 'Sesuai'
    elif any(row.get(col) == 1 for col in row.index if str(col).endswith('_S1')):
        return 'Sangat Sesuai'
    else:
        return 'Tidak Diketahui'

def cari_faktor_pembatas(row):
    kelas = row.get('kelas_kesesuaian', 'Tidak Diketahui')
    if kelas in ['Sangat Sesuai', 'Tidak Diketahui', 'Kawasan Lindung / Tidak Dinilai']:
        return '-'
        
    code_to_name = {v: k for k, v in LAYER_MAP.items()}
    pembatas = []
    
    target_suffix = ""
    if kelas == 'Tidak Sesuai':
        target_suffix = '_N'
    elif kelas == 'Sesuai Marginal':
        target_suffix = '_S3'
    elif kelas == 'Sesuai':
        target_suffix = '_S2'
        
    for col in row.index:
        if str(col).endswith(target_suffix) and row.get(col) == 1:
            param_code = str(col).split("_")[0]
            param_name = code_to_name.get(param_code, param_code)
            if param_name not in pembatas:
                pembatas.append(param_name)
                
    return ', '.join(pembatas) if pembatas else '-'

@app.route("/admin/analisis_otomatis", methods=["POST"])
@admin_required
def analisis_otomatis():
    if not HAS_GEOPANDAS:
        flash("Error: Fitur analisis spasial tidak tersedia di server ini (library GeoPandas tidak terinstall).")
        return redirect(url_for("admin"))
    try:
        gdfs = []
        
        # Eksekusi iteratif per layer untuk mencegah "statement timeout" di Postgres
        for lname, param_code in LAYER_MAP.items():
            conn = get_db_conn()
            # Setel statement timeout agar lebih lama untuk query berat (misal 5 menit)
            with conn.cursor() as timeout_cur:
                timeout_cur.execute("SET statement_timeout = 300000;")
                
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Ambil data HANYA untuk layer yang sesuai nama di LAYER_MAP
            cur.execute("SELECT id, layer_name, geojson FROM layer_peta WHERE layer_name = %s", (lname,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if not rows:
                print(f"Layer '{lname}' tidak ditemukan di database. Melompati...")
                continue
            
            row = rows[0]
            geojson_data = row['geojson']
            
            if not geojson_data or 'features' not in geojson_data or not geojson_data['features']:
                print(f"Layer '{lname}' kosong GeoJSON-nya. Melompati...")
                continue
                
            gdf = gpd.GeoDataFrame.from_features(geojson_data['features'])
            if gdf.empty:
                continue
            if gdf.crs is None:
                gdf.set_crs(epsg=4326, inplace=True)
                
            gdf_norm = normalize_gdf_columns(gdf, param_code)
            gdfs.append(gdf_norm)
            
        if not gdfs:
            flash("Error: Data layer parameter lahan tidak ditemukan atau kosong. Analisis tidak bisa jalan.")
            return redirect(url_for("admin"))
            
        # Lakukan tumpang susun spasial murni (membuang layer admin bawaan)
        kesesuaian = overlay_layers(gdfs)
        if kesesuaian is None or kesesuaian.empty:
            flash("Error: Hasil tumpang susun kosong. Geometri mungkin tidak beririsan sama sekali.")
            return redirect(url_for("admin"))
        
        # Penentuan Kelas dan Pembatas
        kesesuaian['kelas_kesesuaian'] = kesesuaian.apply(tentukan_kelas, axis=1)
        kesesuaian['faktor_pembatas'] = kesesuaian.apply(cari_faktor_pembatas, axis=1)
        
        # Sisa kolom yang penting untuk dicetak
        final_cols = ['kelas_kesesuaian', 'faktor_pembatas', 'geometry']
        kesesuaian = kesesuaian[[c for c in final_cols if c in kesesuaian.columns]]
        
        # Dissolve agar geometri merapat hanya berdasarkan kelas dan faktor pembatas
        gdf_dissolved = kesesuaian.dissolve(by=['kelas_kesesuaian', 'faktor_pembatas'], as_index=False)
        gdf_dissolved = sanitize_geometries(gdf_dissolved)
        
        # ======= TAHAP BARU: OVERLAY DENGAN PETA ADMINISTRASI =======
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Menggunakan data administrasi terbaru (Batas Administrasi Desa)
        cur.execute("SELECT geojson FROM informasi_tambahan WHERE layer_name = 'Batas Administrasi Desa'")
        admin_row = cur.fetchone()
        cur.close()
        conn.close()
        
        if admin_row and admin_row['geojson']:
            admin_gdf = gpd.GeoDataFrame.from_features(admin_row['geojson']['features'])
            if admin_gdf.crs is None:
                admin_gdf.set_crs(epsg=4326, inplace=True)
            
            admin_gdf = sanitize_geometries(admin_gdf)
            # Ambil kolom esensial dari administrasi seperti NAMOBJ atau Kecamatan
            admin_cols = [c for c in admin_gdf.columns if c.lower() in ['namobj', 'kecamatan', 'kabupaten', 'desa', 'kelurahan'] or c == 'geometry']
            admin_gdf = admin_gdf[admin_cols]
            
            # Intersection terakhir untuk menempelkan batas administrasi dan memecah poligon besar
            gdf_dissolved = gpd.overlay(gdf_dissolved, admin_gdf, how='intersection', keep_geom_type=True)
            gdf_dissolved = sanitize_geometries(gdf_dissolved)
            
        # ======= TAHAP BARU: KALKULASI LUAS (HEKTAR) =======
        # Proyeksikan ke EPSG:32750 (UTM Zone 50S untuk Sulawesi) agar kalkulasi meter perseginya akurat
        # Lalu konversi meter persegi (m2) ke Hektar (Ha) dibagi 10000
        gdf_proj = gdf_dissolved.to_crs(epsg=32750)
        gdf_dissolved['Luas (Ha)'] = (gdf_proj.geometry.area / 10000).round(2)
        
        # Convert ke GeoJSON
        geojson_str = gdf_dissolved.to_json()
        geojson_obj = json.loads(geojson_str)
        
        # PROSES MINIFY GEOJSON (menghilangkan spasi, tab, newline yg tidak perlu)
        minified_geojson_str = json.dumps(geojson_obj, separators=(',', ':'))
        
        # Insert ke tabel kesesuaian_lahan dengan Timestamp Nama agar bisa dibedakan
        import datetime
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        layer_name = f"Hasil Analisis Kesesuaian Lahan (Otomatis) - {timestamp_str}"
        desc = "Hasil tumpang susun spasial seluruh parameter lahan, dioverlay dengan administrasi."
        
        conn = get_db_conn()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO kesesuaian_lahan (layer_name, file_name, description, geojson)
            VALUES (%s, %s, %s, %s)
            """,
            (layer_name, f"auto_intersect_{int(datetime.datetime.now().timestamp())}.geojson", desc, minified_geojson_str)
        )
        conn.commit()
        cur.close()
        conn.close()

        flash(f"Analisis Spasial Berhasil! Layer '{layer_name}' telah ditambahkan.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f"Error saat melakukan analisis: {e}")
        
    return redirect(url_for("admin"))

# Upload untuk Parameter (layer_peta)
@app.route("/upload_layer", methods=["POST"])
@admin_required
def upload_layer():
    layer_name = request.form.get("layer_name", "").strip()
    description = request.form.get("description", "").strip()
    file = request.files.get("layer_file")

    success, message = process_upload_layer(layer_name, description, file, "layer_peta")
    flash(message)
    return redirect(url_for("admin"))

# Upload untuk Kesesuaian Lahan
@app.route("/upload_kesesuaian", methods=["POST"])
@admin_required
def upload_kesesuaian():
    layer_name = request.form.get("layer_name", "").strip()
    description = request.form.get("description", "").strip()
    file = request.files.get("layer_file")

    success, message = process_upload_layer(layer_name, description, file, "kesesuaian_lahan")
    flash(message)
    return redirect(url_for("admin"))

# Upload untuk Informasi Tambahan
@app.route("/upload_informasi", methods=["POST"])
@admin_required
def upload_informasi():
    layer_name = request.form.get("layer_name", "").strip()
    description = request.form.get("description", "").strip()
    file = request.files.get("layer_file")

    success, message = process_upload_layer(layer_name, description, file, "informasi_tambahan")
    flash(message)
    return redirect(url_for("admin"))

# -------------------------------------------------
# Hapus layer functions
# -------------------------------------------------
@app.route("/admin/layers/delete/<int:layer_id>", methods=["POST"])
@admin_required
def delete_layer(layer_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM layer_peta WHERE id = %s;", (layer_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Layer berhasil dihapus.")
    return redirect(url_for("admin"))

@app.route("/admin/kesesuaian/delete/<int:layer_id>", methods=["POST"])
@admin_required
def delete_kesesuaian(layer_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM kesesuaian_lahan WHERE id = %s;", (layer_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Layer kesesuaian berhasil dihapus.")
    return redirect(url_for("admin"))

@app.route("/admin/informasi/delete/<int:layer_id>", methods=["POST"])
@admin_required
def delete_informasi(layer_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM informasi_tambahan WHERE id = %s;", (layer_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Layer informasi tambahan berhasil dihapus.")
    return redirect(url_for("admin"))

# -------------------------------------------------
# Manajemen user admin
# -------------------------------------------------
@app.route("/admin/users/add", methods=["POST"])
@admin_required
def admin_add_user():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    is_admin = True if request.form.get("is_admin") == "on" else False

    if not username or not password:
        flash("Username dan password tidak boleh kosong.")
        return redirect(url_for("admin"))

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        pwd_hash = generate_password_hash(password)
        cur.execute(
            """
            INSERT INTO users (username, password_hash, is_admin)
            VALUES (%s, %s, %s);
            """,
            (username, pwd_hash, is_admin),
        )
        conn.commit()
        flash("User admin baru berhasil ditambahkan.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal menambah user: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

@app.route("/admin/users/delete/<int:user_id>", methods=["POST"])
@admin_required
def admin_delete_user(user_id):
    if user_id == session.get("user_id"):
        flash("Tidak bisa menghapus akun yang sedang login.")
        return redirect(url_for("admin"))

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = %s;", (user_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("User berhasil dihapus.")
    return redirect(url_for("admin"))

@app.route("/admin/users/reset_password/<int:user_id>", methods=["POST"])
@admin_required
def admin_reset_user_password(user_id):
    new_password = request.form.get("new_password", "").strip()

    if not new_password:
        flash("Password baru tidak boleh kosong.")
        return redirect(url_for("admin"))

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        pwd_hash = generate_password_hash(new_password)
        cur.execute(
            "UPDATE users SET password_hash = %s WHERE id = %s;",
            (pwd_hash, user_id),
        )
        conn.commit()
        flash("Password user berhasil direset.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal reset password: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

# -------------------------------------------------
# Manajemen Parameter Lahan
# -------------------------------------------------
@app.route("/admin/params/add", methods=["POST"])
@admin_required
def admin_add_param():
    tag_label = request.form.get("tag_label", "").strip()
    icon_class = request.form.get("icon_class", "").strip()
    title = request.form.get("title", "").strip()
    description = request.form.get("description", "").strip()
    order_no = request.form.get("order_no", "0").strip()

    if not tag_label or not title or not description:
        flash("Tag, judul, dan deskripsi parameter wajib diisi.")
        return redirect(url_for("admin"))

    try:
        order_no_int = int(order_no)
    except ValueError:
        order_no_int = 0

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO parameter_lahan
            (tag_label, icon_class, title, description, order_no)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (tag_label, icon_class or None, title, description, order_no_int),
        )
        conn.commit()
        flash("Parameter lahan baru berhasil ditambahkan.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal menambah parameter: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

@app.route("/admin/params/update/<int:param_id>", methods=["POST"])
@admin_required
def admin_update_param(param_id):
    tag_label = request.form.get("tag_label", "").strip()
    icon_class = request.form.get("icon_class", "").strip()
    title = request.form.get("title", "").strip()
    order_no = request.form.get("order_no", "0").strip()

    if not tag_label or not title:
        flash("Tag dan judul parameter wajib diisi.")
        return redirect(url_for("admin"))

    try:
        order_no_int = int(order_no)
    except ValueError:
        order_no_int = 0

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE parameter_lahan
            SET tag_label=%s, icon_class=%s, title=%s, order_no=%s
            WHERE id=%s;
            """,
            (tag_label, icon_class or None, title, order_no_int, param_id),
        )
        conn.commit()
        flash("Parameter lahan berhasil diperbarui.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal update parameter: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

@app.route("/admin/params/delete/<int:param_id>", methods=["POST"])
@admin_required
def admin_delete_param(param_id):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM parameter_lahan WHERE id = %s;", (param_id,))
        conn.commit()
        flash("Parameter lahan berhasil dihapus.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal menghapus parameter: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

@app.route("/admin/params/update_description", methods=["POST"])
@admin_required
def admin_update_param_description():
    param_id = request.form.get("param_id", "").strip()
    description = request.form.get("description", "").strip()

    if not param_id:
        flash("Parameter tidak valid.")
        return redirect(url_for("admin"))

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE parameter_lahan SET description = %s WHERE id = %s;",
            (description, int(param_id)),
        )
        conn.commit()
        flash("Deskripsi parameter berhasil diperbarui.")
    except psycopg2.Error as e:
        conn.rollback()
        flash(f"Gagal update deskripsi: {e}")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("admin"))

# -------------------------------------------------
# API: LIST & GEOJSON
# -------------------------------------------------
@app.route("/api/layers", methods=["GET"])
def api_layers():
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT id, layer_name, description, created_at
        FROM layer_peta
        ORDER BY created_at DESC;
        """
    )
    layers = cur.fetchall()

    cur.close()
    conn.close()
    return jsonify(layers)

@app.route("/api/layers/<int:layer_id>")
def api_layer_geojson(layer_id):
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT id, layer_name, geojson
        FROM layer_peta
        WHERE id = %s;
        """,
        (layer_id,),
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Layer tidak ditemukan"}), 404

    return jsonify(row["geojson"])

# API untuk Kesesuaian Lahan
@app.route("/api/kesesuaian/<int:layer_id>")
def api_kesesuaian_geojson(layer_id):
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT id, layer_name, geojson
        FROM kesesuaian_lahan
        WHERE id = %s;
        """,
        (layer_id,),
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Layer tidak ditemukan"}), 404

    return jsonify(row["geojson"])

# API untuk Informasi Tambahan
@app.route("/api/informasi/<int:layer_id>")
def api_informasi_geojson(layer_id):
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT id, layer_name, geojson
        FROM informasi_tambahan
        WHERE id = %s;
        """,
        (layer_id,),
    )
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Layer tidak ditemukan"}), 404

    return jsonify(row["geojson"])

@app.route("/api/distribusi_kesesuaian", methods=["GET"])
def distribusi_kesesuaian():
    if not HAS_GEOPANDAS:
        return jsonify({"error": "Library GeoPandas tidak tersedia di server ini."}), 503
    conn = get_db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Ambil GeoJSON dari tabel kesesuaian_lahan (Prioritaskan Hasil Analisis Otomatis terbaru)
    cur.execute(
        """
        SELECT geojson 
        FROM kesesuaian_lahan 
        WHERE layer_name LIKE 'Hasil Analisis Kesesuaian Lahan (Otomatis)%' 
        ORDER BY id DESC LIMIT 1;
        """
    )
    row = cur.fetchone()
    
    # Fallback ke data lain jika tidak ada analisis otomatis
    if not row:
        cur.execute("SELECT geojson FROM kesesuaian_lahan ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Data tidak ditemukan"}), 404

    try:
        # Load geojson ke GeoDataFrame
        gdf = gpd.GeoDataFrame.from_features(row["geojson"]["features"])
        
        # Pastikan CRS WGS84 → ubah ke meter (UTM 50S = 32750)
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)

        gdf = gdf.to_crs(epsg=32750)

        # Hitung luas dalam hektar
        gdf["luas_ha"] = gdf.geometry.area / 10000

        # Normalisasi nama kolom - cari kolom kelas, kecamatan, dan desa/kelurahan
        kelas_col = None
        kecamatan_col = None
        desa_col = None
        
        for col in gdf.columns:
            col_lower = str(col).lower()
            
            # Cari kolom kelas
            if ('kelas' in col_lower or 'kesesuaian' in col_lower) and kelas_col is None:
                kelas_col = col
            
            # Cari kolom kecamatan
            if any(x in col_lower for x in ['kecamatan', 'wadmkc']) and kecamatan_col is None:
                kecamatan_col = col
                
            # Cari kolom desa/kelurahan (di peta administrasi sering bernama namobj, desa, atau wadmdes)
            if any(x in col_lower for x in ['desa', 'kelurahan', 'namobj', 'wadmdes']) and desa_col is None:
                # Tapi kalau ini sama dengan kecamatan, jangan diambil
                if col != kecamatan_col:
                    desa_col = col
        
        if not kelas_col:
            return jsonify({"error": "Kolom 'kelas' tidak ditemukan", "available_columns": gdf.columns.tolist()}), 400
            
        if not kecamatan_col:
            return jsonify({"error": "Kolom kecamatan tidak ditemukan", "available_columns": gdf.columns.tolist()}), 400
            
        # Logika Fallback jika desa_col tidak ditemukan karena NAMOBJ termakan oleh kecamatan
        if not desa_col:
            for col in gdf.columns:
                if 'namobj' in str(col).lower() and col != kecamatan_col:
                    desa_col = col
                    break
        
        # Buat kolom standar
        gdf["kelas"] = gdf[kelas_col].fillna("NONE")
        gdf["kecamatan"] = gdf[kecamatan_col].fillna("Tidak diketahui")
        gdf["desa"] = gdf[desa_col].fillna("Tidak diketahui") if desa_col else "Tidak dketahui"

        # Group by gabungan untuk mencover Kecamatan dan Desa
        summary = (
            gdf.groupby(["kecamatan", "desa", "kelas"])["luas_ha"]
            .sum()
            .reset_index()
            .sort_values(["kecamatan", "desa", "kelas"])
        )

        result = summary.to_dict(orient="records")

        return jsonify(result)
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# -------------------------------------------------
# Chatbot n8n Proxy (opsional)
# -------------------------------------------------
@app.route("/api/chatbot", methods=["POST"])
def chatbot_proxy():
    user_msg = request.json.get("message")

    resp = requests.post(
        "https://n8n-hkpugvrhtnxs.ceri.sumopod.my.id/webhook/4091fa09-fb9a-4039-9411-7104d213f601/chat",
        json={"message": user_msg},
        timeout=20,
    )

    return jsonify(resp.json())

# -------------------------------------------------
# INIT DB saat module dimuat (untuk Vercel cold start)
# -------------------------------------------------
try:
    init_db()
except Exception as e:
    print(f"Warning: init_db gagal: {e}")

# -------------------------------------------------
# RUN (hanya untuk development lokal)
# -------------------------------------------------
if __name__ == "__main__":
    print("FLASK SIAP JALAN...")
    app.run(debug=True, port=5001)