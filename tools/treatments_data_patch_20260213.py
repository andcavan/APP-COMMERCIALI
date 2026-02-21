from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unificati_manager.db import Database
from unificati_manager.utils import normalize_upper


DB_PATH = ROOT / "unificati_manager" / "database" / "unificati_manager.db"
BACKUP_DIR = ROOT / "unificati_manager" / "backups"


@dataclass(frozen=True)
class TreatmentEntry:
    code: str
    description: str
    characteristics: str
    standard: str
    notes: str


HEAT_TREATMENTS: List[TreatmentEntry] = [
    TreatmentEntry(
        code="HEAT_BONIFICA",
        description="BONIFICA (TEMPRA + RINVENIMENTO)",
        characteristics=(
            "OBIETTIVO: ALTA RESISTENZA MECCANICA CON TENACITA ADEGUATA.\n"
            "APPLICABILITA: ACCIAI DA BONIFICA NON LEGATI E LEGATI (ES. C45, 42CRMO4, 39NICRMO3).\n"
            "PROCESSO: AUSTENITIZZAZIONE, TEMPRA (OLIO/POLIMERO/ACQUA), RINVENIMENTO.\n"
            "NOTE: IL RINVENIMENTO REGOLA IL COMPROMESSO DUREZZA/TENACITA."
        ),
        standard="ISO 683-1:2016 / ISO 4885:2018 / ISO 18203:2026",
        notes="TRATTAMENTO PER COMPONENTI STRUTTURALI, ALBERI, INGRANAGGI, ORGANI DI TRASMISSIONE.",
    ),
    TreatmentEntry(
        code="HEAT_TEMPRA",
        description="TEMPRA (GENERICA)",
        characteristics=(
            "OBIETTIVO: OTTENERE STRUTTURA MARTENSITICA AD ALTA DUREZZA.\n"
            "APPLICABILITA: ACCIAI IDONEI ALLA TEMPRA.\n"
            "PROCESSO: RISCALDO DI AUSTENITIZZAZIONE + RAFFREDDAMENTO RAPIDO.\n"
            "NOTE: SPESSO SEGUITA DA RINVENIMENTO PER RIDURRE FRAGILITA."
        ),
        standard="ISO 4885:2018 / ISO 683-1:2016",
        notes="USARE CON CONTROLLO DI DISTORSIONE E RISCHIO CRICCHE DA TEMPRA.",
    ),
    TreatmentEntry(
        code="HEAT_RINV",
        description="RINVENIMENTO",
        characteristics=(
            "OBIETTIVO: RIDURRE TENSIONI E FRAGILITA DOPO TEMPRA.\n"
            "APPLICABILITA: COMPONENTI TEMPRATI, BONIFICATI, INDURITI LOCALMENTE.\n"
            "PROCESSO: RISCALDO CONTROLLATO SOTTO AC1 + RAFFREDDAMENTO.\n"
            "NOTE: RIDUCE DUREZZA MA AUMENTA TENACITA E STABILITA."
        ),
        standard="ISO 4885:2018",
        notes="PARAMETRI DI RINVENIMENTO DEFINITI DA CLASSE ACCIAIO E TARGET MECCANICO.",
    ),
    TreatmentEntry(
        code="HEAT_CEM",
        description="CEMENTAZIONE + TEMPRA",
        characteristics=(
            "OBIETTIVO: SUPERFICIE DURA CON CUORE TENACE.\n"
            "APPLICABILITA: ACCIAI DA CEMENTAZIONE A BASSO TENORE DI CARBONIO.\n"
            "PROCESSO: DIFFUSIONE C (TIPICO 880-980 C), TEMPRA E RINVENIMENTO.\n"
            "RANGE TIPICI: PROFONDITA EFFICACE DA STRATO SOTTILE FINO A CASI PROFONDI."
        ),
        standard="ISO 683-3:2022 / ISO 18203:2026",
        notes="IDEALE PER INGRANAGGI, PIGNONI, CAMME, PERNI, COMPONENTI AD USURA.",
    ),
    TreatmentEntry(
        code="HEAT_CEM_LPC",
        description="CEMENTAZIONE A BASSA PRESSIONE (LPC)",
        characteristics=(
            "OBIETTIVO: CEMENTAZIONE IN VUOTO CON MAGGIORE PULIZIA METALLURGICA.\n"
            "APPLICABILITA: COMPONENTI COMPLESSI, FORI CIECHI, GEOMETRIE FINI.\n"
            "PROCESSO: CICLI BOOST/DIFFUSIONE IN VUOTO, TEMPRA IN GAS O OLIO.\n"
            "NOTE: RIDUCE OSSIDAZIONE E PUO LIMITARE DISTORSIONE."
        ),
        standard="ISO 18203:2026 / ISO 683-3:2022",
        notes="USATA SU COMPONENTI DI PRECISIONE DOVE SERVE CONTROLLO STRATO E PULIZIA SUPERFICIALE.",
    ),
    TreatmentEntry(
        code="HEAT_CARBONIT",
        description="CARBONITRURAZIONE",
        characteristics=(
            "OBIETTIVO: INDURIMENTO SUPERFICIALE TERMOCHEMICO CON C + N.\n"
            "APPLICABILITA: PICCOLA/MEDIA MINUTERIA, COMPONENTI CON ESIGENZA BASSA DISTORSIONE.\n"
            "PROCESSO: TIPICAMENTE 820-900 C, POI TEMPRA E RINVENIMENTO.\n"
            "RANGE TIPICI: CHD SPESSO NELL'ORDINE DECIMI DI MM."
        ),
        standard="ISO 18203:2026 / ISO 683-3:2022",
        notes="ADATTA QUANDO SERVE DUREZZA SUPERFICIALE E RESISTENZA A FATICA CONTATTO.",
    ),
    TreatmentEntry(
        code="HEAT_NIT_GAS",
        description="NITRURAZIONE GASSOSA",
        characteristics=(
            "OBIETTIVO: ELEVATA DUREZZA SUPERFICIALE CON DISTORSIONE CONTENUTA.\n"
            "APPLICABILITA: ACCIAI NITRURABILI, STAMPI, ALBERI, ORGANI DI SCORRIMENTO.\n"
            "PROCESSO: DIFFUSIONE AZOTO A TEMPERATURA MODERATA, SENZA TEMPRA FINALE.\n"
            "NOTE: OTTIMA RESISTENZA USURA E FATICA SUPERFICIALE."
        ),
        standard="ISO 18203:2026 / ISO 4885:2018",
        notes="RICHIEDE STATO METALLURGICO DI PARTENZA ADEGUATO E CONTROLLO DELLO STRATO COMPOSTO.",
    ),
    TreatmentEntry(
        code="HEAT_NIT_PLASMA",
        description="NITRURAZIONE IONICA (PLASMA)",
        characteristics=(
            "OBIETTIVO: STRATO NITRURATO UNIFORME E CONTROLLATO.\n"
            "APPLICABILITA: ACCIAI ALLOY, INOX IDONEI, COMPONENTI DI PRECISIONE.\n"
            "PROCESSO: VUOTO + SCARICA AL PLASMA PER ATTIVARE LA DIFFUSIONE DI AZOTO.\n"
            "NOTE: BUON CONTROLLO DELLA MICROSTRUTTURA E LIMITATA DISTORSIONE."
        ),
        standard="ISO 18203:2026 / ISO 4885:2018",
        notes="PARTICOLARMENTE UTILE QUANDO SERVONO TOLLERANZE STRETTE E FINITURA PRESERVATA.",
    ),
    TreatmentEntry(
        code="HEAT_IND",
        description="TEMPRA A INDUZIONE",
        characteristics=(
            "OBIETTIVO: INDURIMENTO LOCALIZZATO DELLE SUPERFICI FUNZIONALI.\n"
            "APPLICABILITA: ALBERI, DENTATURE, PISTE CUSCINETTO, CAMME.\n"
            "PROCESSO: RISCALDO RAPIDO A INDUZIONE + TEMPRA IMMEDIATA + EVENTUALE RINVENIMENTO.\n"
            "NOTE: CUORE RESTA TENACE CON STRATO DURO SUPERFICIALE."
        ),
        standard="ISO 18203:2026 / ISO 683-1:2016",
        notes="TRATTAMENTO SELETTIVO ADATTO A ZONE CRITICHE CON CARICO DA CONTATTO.",
    ),
    TreatmentEntry(
        code="HEAT_FLAME",
        description="TEMPRA A FIAMMA",
        characteristics=(
            "OBIETTIVO: INDURIMENTO SUPERFICIALE LOCALIZZATO CON ATTREZZATURA SEMPLICE.\n"
            "APPLICABILITA: PEZZI MEDI/GRANDI DOVE NON E ECONOMICA L'INDUZIONE.\n"
            "PROCESSO: RISCALDO A FIAMMA OSSIGAS + TEMPRA.\n"
            "NOTE: MAGGIORE VARIABILITA TERMICA RISPETTO A INDUZIONE."
        ),
        standard="ISO 4885:2018 / ISO 18203:2026",
        notes="RICHIEDE PROCESSO BEN QUALIFICATO PER RIPETIBILITA E DISTORSIONE.",
    ),
    TreatmentEntry(
        code="HEAT_NORM",
        description="NORMALIZZAZIONE",
        characteristics=(
            "OBIETTIVO: RAFFINARE IL GRANO E OMOGENEIZZARE STRUTTURA.\n"
            "APPLICABILITA: SEMILAVORATI LAMINATI/FORGIATI PRIMA DI LAVORAZIONI O TRATTAMENTI SUCCESSIVI.\n"
            "PROCESSO: RISCALDO SOPRA AC3/ACM + RAFFREDDAMENTO IN ARIA.\n"
            "NOTE: MIGLIORA UNIFORMITA MECCANICA."
        ),
        standard="ISO 4885:2018",
        notes="SPESSO USATA COME STATO INTERMEDIO PRIMA DI BONIFICA O INDURIMENTI SUPERFICIALI.",
    ),
    TreatmentEntry(
        code="HEAT_RIC",
        description="RICOTTURA (GENERICA)",
        characteristics=(
            "OBIETTIVO: RIDURRE DUREZZA E MIGLIORARE LAVORABILITA.\n"
            "APPLICABILITA: ACCIAI E LEGHE DOPO LAMINAZIONE, SALDATURA O LAVORAZIONI PLASTICHE.\n"
            "PROCESSO: CICLO TERMICO CON RAFFREDDAMENTO CONTROLLATO/LENTO.\n"
            "NOTE: INCLUDE VARIANTI DI ADDOLCIMENTO, OMOGENIZZAZIONE, GLOBALE O LOCALE."
        ),
        standard="ISO 4885:2018",
        notes="DEFINIRE A DISEGNO IL TIPO DI RICOTTURA E L'OBIETTIVO METALLURGICO.",
    ),
    TreatmentEntry(
        code="HEAT_DIST",
        description="DISTENSIONE (STRESS RELIEVING)",
        characteristics=(
            "OBIETTIVO: RIDURRE TENSIONI RESIDUE DA SALDATURA, LAVORAZIONI O TEMPRA.\n"
            "APPLICABILITA: PEZZI SALDATI, GREZZI SGROSSATI, COMPONENTI TEMPRATI PRIMA FINITURA.\n"
            "PROCESSO: RISCALDO CONTROLLATO SOTTO LE TEMPERATURE DI TRASFORMAZIONE + RAFFREDDAMENTO LENTO.\n"
            "NOTE: MIGLIORA STABILITA DIMENSIONALE."
        ),
        standard="ISO 4885:2018",
        notes="PRATICA DI PREVENZIONE DISTORSIONI PRIMA DI RETTIFICA/FRESATURA FINALE.",
    ),
]


SURFACE_TREATMENTS: List[TreatmentEntry] = [
    TreatmentEntry(
        code="SURF_ANOD",
        description="ANODIZZAZIONE (OSSIDAZIONE ANODICA)",
        characteristics=(
            "OBIETTIVO: PROTEZIONE CORROSIONE + FINITURA ESTETICA SU ALLUMINIO.\n"
            "APPLICABILITA: LEGHE DI ALLUMINIO, COMPONENTI MECCANICI E ARCHITETTURALI.\n"
            "PROCESSO: CRESCITA ELETTROLITICA DI OSSIDO ANODICO INTEGRATO AL SUBSTRATO.\n"
            "NOTE: LO STRATO NON SI SFOGLIA COME UNA VERNICE E PUO ESSERE SIGILLATO."
        ),
        standard="ISO 7599:2018",
        notes="POSSIBILE COLORAZIONE E CONTROLLO CLASSE SPESSORE/QUALITA IN FUNZIONE DELL'USO.",
    ),
    TreatmentEntry(
        code="SURF_ANOD_HARD",
        description="ANODIZZAZIONE DURA (HARD ANODIZING)",
        characteristics=(
            "OBIETTIVO: ALTA RESISTENZA A USURA E ABRASIONE SU ALLUMINIO.\n"
            "APPLICABILITA: PISTONI, GUIDE, SLITTE, PARTI IN SCORRIMENTO O ABRASIONE.\n"
            "PROCESSO: OSSIDAZIONE ANODICA IN CONDIZIONI PIU SPINTE RISPETTO ANODIZZAZIONE DECORATIVA.\n"
            "NOTE: PUO AUMENTARE ISOLAMENTO ELETTRICO E DUREZZA SUPERFICIALE."
        ),
        standard="ISO 10074:2021",
        notes="VERIFICARE TOLLERANZE: LO STRATO HA CRESCITA SIA VERSO ESTERNO SIA VERSO INTERNO.",
    ),
    TreatmentEntry(
        code="SURF_BRUN",
        description="BRUNITURA (OSSIDO NERO)",
        characteristics=(
            "OBIETTIVO: FINITURA NERA UNIFORME CON PROTEZIONE BASE.\n"
            "APPLICABILITA: ACCIAI/CEMENTATI/UTENSILI CON ESIGENZA ESTETICA E ANTI-RIFLESSO.\n"
            "PROCESSO: FORMAZIONE STRATO DI OSSIDO NERO (CONVERSIONE CHIMICA).\n"
            "NOTE: RESISTENZA CORROSIONE LIMITATA SE NON ABBINATA A OLIO/CERA/SIGILLANTI."
        ),
        standard="ISO 11408:1999",
        notes="TRATTAMENTO SOTTILE, NON ADATTO DA SOLO AD AMBIENTI ALTAMENTE CORROSIVI.",
    ),
    TreatmentEntry(
        code="SURF_ZN_HOT",
        description="ZINCATURA A CALDO",
        characteristics=(
            "OBIETTIVO: PROTEZIONE A LUNGO TERMINE CONTRO CORROSIONE.\n"
            "APPLICABILITA: CARPENTERIA, STRUTTURE, BULLONERIA, AMBIENTI ESTERNI.\n"
            "PROCESSO: IMMERSIONE IN ZINCO FUSO; PROTEZIONE BARRIERA + SACRIFICA ANODICA.\n"
            "NOTE: SPESSORI MAGGIORI RISPETTO ZINCATURA ELETTROLITICA."
        ),
        standard="ISO 1461:2022 / ISO 14713-2:2019",
        notes="OTTIMA DURABILITA IN ATMOSFERE INDUSTRIALI/MARINE CON CORRETTA PROGETTAZIONE.",
    ),
    TreatmentEntry(
        code="SURF_ZN_ELEC",
        description="ZINCATURA ELETTROLITICA",
        characteristics=(
            "OBIETTIVO: PROTEZIONE CORROSIONE CON BUONA FINITURA ESTETICA E TOLLERANZE STRETTE.\n"
            "APPLICABILITA: MINUTERIA, VITERIA, PARTICOLARI MECCANICI DI PRECISIONE.\n"
            "PROCESSO: DEPOSIZIONE ELETTROLITICA DI ZINCO + PASSIVAZIONE/SEALER.\n"
            "NOTE: SPESSORI TIPICI PIU BASSI DELLA ZINCATURA A CALDO."
        ),
        standard="ISO 2081:2025 / ASTM B633-23",
        notes="ATTENZIONE A FRAGILITA DA IDROGENO SU ACCIAI AD ALTA RESISTENZA (RICHIEDE DE-EMBRITTLEMENT).",
    ),
    TreatmentEntry(
        code="SURF_ZN_ELEC_FREE",
        description="ZINCATURA ELETTROLITICA CR(VI)-FREE (ZN-NI / ZN-FE)",
        characteristics=(
            "OBIETTIVO: PROTEZIONE CORROSIONE CON SISTEMI SENZA CROMO ESAVALENTE.\n"
            "APPLICABILITA: AUTOMOTIVE, COMPONENTI CON VINCOLI ROHS/REACH, BULLONERIA TECNICA.\n"
            "PROCESSO: LEGHE ZN-NI O ZN-FE + PASSIVAZIONI CR(III) + TOPCOAT.\n"
            "NOTE: CLASSE PRESTAZIONALE ALTA IN NEBBIA SALINA SU CICLI QUALIFICATI."
        ),
        standard="ISO 19598:2016 / ISO 2081:2025",
        notes="USARE QUANDO RICHIESTA CONFORMITA REACH E PRESTAZIONE ANTICORROSIVA ELEVATA.",
    ),
    TreatmentEntry(
        code="SURF_HC",
        description="CROMATURA A SPESSORE (HARD CHROME)",
        characteristics=(
            "OBIETTIVO: ELEVATA RESISTENZA A USURA, GRIPPAGGIO E FATICA DI CONTATTO.\n"
            "APPLICABILITA: STELI, RULLI, CILINDRI, ALBERI, SUPERFICI DI SCORRIMENTO.\n"
            "PROCESSO: DEPOSIZIONE ELETTROLITICA DI CROMO PER USO INGEGNERISTICO.\n"
            "NOTE: SPESSORE E FINITURA VANNO SPECIFICATI A DISEGNO."
        ),
        standard="ISO 6158:2018",
        notes="PREVEDERE EVENTUALE RETTIFICA/LAPPATURA POST-DEPOSITO PER RUGOSITA E GEOMETRIA.",
    ),
    TreatmentEntry(
        code="SURF_CR_DEC",
        description="CROMATURA DECORATIVA (NI + CR)",
        characteristics=(
            "OBIETTIVO: ASPETTO DECORATIVO BRILLANTE + PROTEZIONE CORROSIONE.\n"
            "APPLICABILITA: PARTICOLARI ESTETICI, ACCESSORI, COMPONENTI VISIBILI.\n"
            "PROCESSO: STRATI MULTIPLI DI NICHEL + CROMO SOTTILE FINALE.\n"
            "NOTE: DIVERSA DALLA CROMATURA A SPESSORE PER FUNZIONE E SPESSORE."
        ),
        standard="ISO 1456:2009",
        notes="NON DESTINATA A CARICHI DI USURA SEVERA COME HARD CHROME INGEGNERISTICO.",
    ),
    TreatmentEntry(
        code="SURF_NI_CHEM",
        description="NICHELATURA CHIMICA (NI-P, ELECTROLESS)",
        characteristics=(
            "OBIETTIVO: RIVESTIMENTO UNIFORME ANCHE SU GEOMETRIE COMPLESSE.\n"
            "APPLICABILITA: VALVOLE, CORPI FORATI, COMPONENTI CON CAVITA E FILETTATURE.\n"
            "PROCESSO: DEPOSIZIONE AUTOCATALITICA NI-P SENZA CORRENTE.\n"
            "NOTE: DOPO TRATTAMENTO TERMICO PUO MIGLIORARE DUREZZA E USURA."
        ),
        standard="ISO 4527:2003",
        notes="OTTIMA UNIFORMITA SPESSORE SU FORI CIECHI E PROFILI COMPLESSI RISPETTO ELETTROLITICO.",
    ),
    TreatmentEntry(
        code="SURF_PHOS",
        description="FOSFATAZIONE",
        characteristics=(
            "OBIETTIVO: STRATO DI CONVERSIONE PER ADESIONE VERNICI/LUBRIFICAZIONE E PROTEZIONE BASE.\n"
            "APPLICABILITA: ACCIAI, ZINCATI, ALCUNE LEGHE DI ALLUMINIO IN CICLI QUALIFICATI.\n"
            "PROCESSO: CONVERSIONE CHIMICA A BASE FOSFATO (ZN/MN/FE).\n"
            "NOTE: SPESSO PRE-TRATTAMENTO PRIMA DI VERNICIATURA O FORMATURA A FREDDO."
        ),
        standard="ISO 9717:2024",
        notes="ABBINARE A OLIO/SEALER O VERNICE PER PROTEZIONE CORROSIVA PIU ELEVATA.",
    ),
    TreatmentEntry(
        code="SURF_PASS_INOX",
        description="PASSIVAZIONE ACCIAI INOX",
        characteristics=(
            "OBIETTIVO: RIPRISTINARE/MIGLIORARE FILM PASSIVO DOPO LAVORAZIONI O CONTAMINAZIONI FERROSE.\n"
            "APPLICABILITA: AISI 304, 316, DUPLEX E LEGHE INOX IDONEE.\n"
            "PROCESSO: TRATTAMENTO CHIMICO DI DECONTAMINAZIONE + PASSIVAZIONE.\n"
            "NOTE: NON E UN RIVESTIMENTO, E OTTIMIZZAZIONE DELLO STRATO NATIVO."
        ),
        standard="ISO 16048:2003 / ASTM A380/A380M-25 / ASTM A967/A967M",
        notes="UTILIZZARE DOPO SALDATURA, DECAPAGGIO O LAVORAZIONI CHE LASCIANO CONTAMINANTI FERROSI.",
    ),
]


def ensure_unique_code(cur, table: str, desired: str) -> str:
    base = normalize_upper(desired)
    cur.execute(f"SELECT id FROM {table} WHERE code=?", (base,))
    if cur.fetchone() is None:
        return base
    idx = 2
    while True:
        candidate = f"{base}_{idx:02d}"
        cur.execute(f"SELECT id FROM {table} WHERE code=?", (candidate,))
        if cur.fetchone() is None:
            return candidate
        idx += 1


def upsert_treatments(cur, table: str, entries: List[TreatmentEntry]) -> Dict[str, int]:
    created = 0
    updated = 0
    for e in entries:
        desc = normalize_upper(e.description)
        chars = normalize_upper(e.characteristics)
        std = normalize_upper(e.standard)
        notes = normalize_upper(e.notes)

        cur.execute(f"SELECT id, code FROM {table} WHERE description=?", (desc,))
        row = cur.fetchone()
        if row is not None:
            cur.execute(
                f"""
                UPDATE {table}
                SET description=?, characteristics=?, standard=?, notes=?, is_active=1, updated_at=?
                WHERE id=?
                """,
                (desc, chars, std, notes, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(row["id"])),
            )
            updated += 1
            continue

        code = ensure_unique_code(cur, table, e.code)
        cur.execute(
            f"""
            INSERT INTO {table}(code, description, characteristics, standard, notes, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                code,
                desc,
                chars,
                std,
                notes,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        created += 1
    return {"created": created, "updated": updated}


def run_patch(apply_changes: bool) -> int:
    db = Database(str(DB_PATH))
    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"unificati_manager_backup_{stamp}_treatments_patch.db"
        db.backup_to_path(str(backup_path))
        print(f"Backup: {backup_path}")

        cur = db.conn.cursor()
        # allinea transazioni implicite
        db.conn.commit()
        if apply_changes:
            db.conn.execute("BEGIN")

        heat_stats = upsert_treatments(cur, "heat_treatment", HEAT_TREATMENTS)
        surf_stats = upsert_treatments(cur, "surface_treatment", SURFACE_TREATMENTS)

        if apply_changes:
            db.conn.commit()
        else:
            db.conn.rollback()

        print(f"Mode: {'APPLY' if apply_changes else 'DRY-RUN'}")
        print(f"Heat treatments: created={heat_stats['created']} updated={heat_stats['updated']}")
        print(f"Surface treatments: created={surf_stats['created']} updated={surf_stats['updated']}")
        print(f"Total definitions processed: {len(HEAT_TREATMENTS) + len(SURFACE_TREATMENTS)}")
        return 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import or update heat/surface treatment master data.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag runs in dry-run mode.")
    args = parser.parse_args()
    return run_patch(apply_changes=bool(args.apply))


if __name__ == "__main__":
    raise SystemExit(main())
