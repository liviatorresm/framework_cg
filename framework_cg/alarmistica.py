import logging
import sys
from pathlib import Path
import traceback
import getpass
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, Deque, Iterable, Any
from collections import deque
import os
from dotenv import load_dotenv

load_dotenv('config/.env')

# =============================
# Setup de logging por script
# =============================
def setup_script_logger(
    logs_dir: str | None = None,
    script_name: Optional[str] = None,
    overwrite: bool = True,
    level: int = logging.INFO,
    fmt: str = "%(asctime)s - %(levelname)s - %(message)s",
):
    """
    Configura logger para scripts Dockerizados com rotação de arquivo.
    - Se overwrite=True, apaga o arquivo anterior e inicia novo log.
    - Sempre escreve também no stdout.
    """

    # Diretório base
    logs_dir = logs_dir or os.getenv("LOG_DIR", "/app/logs")
    if not Path(logs_dir).exists() and logs_dir == "/app/logs":
        logs_dir = "logs"
    Path(logs_dir).mkdir(parents=True, exist_ok=True)

    # Nome do log = nome script
    if script_name is None:
        script_name = Path(sys.argv[0]).stem or "app"

    log_path = Path(logs_dir) / f"{script_name}.log"

    # Apagar log antigos
    if overwrite and log_path.exists():
        try:
            log_path.unlink()
        except Exception as e:
            print(f"[WARN] Falha ao limpar log antigo {log_path}: {e}")

    logger = logging.getLogger(script_name)
    logger.setLevel(level)
    logger.propagate = False

    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    from logging.handlers import RotatingFileHandler
    file_mode = "w" if overwrite else "a"
    fh = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
        mode=file_mode
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger, str(log_path)

# =====================================
# ProcessLogger com integração ao banco
# =====================================
class ProcessLogger:
    """
    Integra logging local (arquivo/console) com:
      - processamento (linha por execução)
      - processamento_log (linha por evento relevante)
    Adapta os SQLs se a coluna 'mensagem' não existir.
    """

    def __init__(
        self,
        usuario: Optional[str],
        db_name: str,
        logger: Optional[logging.Logger] = None,
        max_db_chars: int = 4000,
        log_to_db_levels: Iterable[str] = ("WARN", "ERROR", "FATAL"),
    ):
        self.db_name = db_name
        self.usuario = usuario or getpass.getuser()
        self.logger = logger or logging.getLogger(Path(sys.argv[0]).stem or "app")
        self._buffer: Deque[str] = deque(maxlen=5000)
        self._max_db_chars = max_db_chars
        self._log_to_db_levels = {self._norm_level(lvl) for lvl in log_to_db_levels}
        self._processamento_id: Optional[int] = None
        self._has_mensagem_col: Optional[bool] = None  

    @staticmethod
    def _coerce_logger(logger_like: Any) -> logging.Logger:
        if isinstance(logger_like, logging.Logger):
            return logger_like
        if isinstance(logger_like, (tuple, list)):
            for item in logger_like:
                if hasattr(item, "info") and hasattr(item, "warning") and hasattr(item, "error"):
                    return item  # primeiro logger válido
        if hasattr(logger_like, "info") and hasattr(logger_like, "warning") and hasattr(logger_like, "error"):
            return logger_like  # objeto compatível
        # fallback seguro
        return logging.getLogger("alarmistica")
    
    # ---------- util ----------
    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _norm_level(level: str) -> str:
        m = str(level).upper()
        if m in {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}: return m
        if m == "WARNING": return "WARN"
        if m == "CRITICAL": return "FATAL"
        return "INFO"

    @staticmethod
    def _status_success() -> str: return "SUCCESS"
    @staticmethod
    def _status_running() -> str: return "RUNNING"
    @staticmethod
    def _status_error() -> str:   return "ERROR"

    def _buffer_compacto(self) -> str:
        full = "\n".join(self._buffer)
        if len(full) <= self._max_db_chars: return full
        head = "... (truncado) ...\n"
        return head + full[-(self._max_db_chars - len(head)) :]

    # ---------- DB ops ----------
    def _get_conn(self):
        from framework_cg.conn import PostgresConnection
        db = PostgresConnection()
        return db.conectar_postgres(db_name=self.db_name)

    def _ensure_schema_introspection(self) -> None:
        if self._has_mensagem_col is not None:
            return
        try:
            with self._get_conn() as conn:
                if conn is None:
                    self.logger.error("Conexão ao banco falhou (introspecção).")
                    self._has_mensagem_col = False
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1
                          FROM information_schema.columns
                         WHERE table_name = 'processamento'
                           AND column_name = 'mensagem'
                           AND table_schema = current_schema()
                        LIMIT 1;
                    """)
                    self._has_mensagem_col = cur.fetchone() is not None
            if not self._has_mensagem_col:
                self.logger.info("Tabela processamento sem coluna 'mensagem' — SQL será adaptado.")
        except Exception as e:
            self.logger.warning("Falha ao inspecionar schema (assumindo sem 'mensagem'): %s: %s",
                                type(e).__name__, e)
            self._has_mensagem_col = False

    def log_mensagem(self, mensagem: str, level: str = "info",
                     etapa: Optional[str] = None, codigo: Optional[str] = None,
                     detalhe: Optional[dict] = None) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lvl = self._norm_level(level)
        linha = f"{ts} - {lvl} - {mensagem}"
        self._buffer.append(linha)

        if lvl == "ERROR":   self.logger.error(mensagem)
        elif lvl == "WARN":  self.logger.warning(mensagem)
        elif lvl == "DEBUG": self.logger.debug(mensagem)
        elif lvl == "FATAL": self.logger.critical(mensagem)
        else:                self.logger.info(mensagem)

        if self._processamento_id and (lvl in self._log_to_db_levels):
            self.registrar_evento(
                processamento_id=self._processamento_id,
                level=lvl, etapa=etapa, codigo=codigo,
                mensagem=mensagem, detalhe=detalhe, stacktrace=None
            )

    def registrar_inicio(self, nome_processo: str, usuario: Optional[str] = None) -> Optional[int]:
        """Cria linha RUNNING e retorna id."""
        try:
            self._ensure_schema_introspection()
            inicio = self._now_utc()
            fim = inicio
            usuario = usuario or self.usuario
            with self._get_conn() as conn:
                if conn is None:
                    self.logger.error("Conexão ao banco falhou.")
                    return None
                with conn.cursor() as cur:
                    if self._has_mensagem_col:
                        cur.execute(
                            """
                            INSERT INTO processamento (nome_processo, inicio, fim, usuario, status, mensagem)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (nome_processo, inicio, fim, usuario, self._status_running(), "(início da execução)"),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO processamento (nome_processo, inicio, fim, usuario, status)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (nome_processo, inicio, fim, usuario, self._status_running()),
                        )
                    new_id = cur.fetchone()[0]
                conn.commit()
            self._processamento_id = new_id
            return new_id
        except Exception as e:
            self.logger.error("Erro ao registrar início: %s: %s", type(e).__name__, e)
            return None

    def atualizar_execucao(self, processamento_id: int, inicio: datetime, fim: datetime,
                           status: str, mensagem: Optional[str]) -> None:
        try:
            self._ensure_schema_introspection()
            with self._get_conn() as conn:
                if conn is None:
                    self.logger.error("Conexão ao banco falhou (update).")
                    return
                with conn.cursor() as cur:
                    if self._has_mensagem_col:
                        cur.execute(
                            """
                            UPDATE processamento
                               SET inicio = %s,
                                   fim = %s,
                                   status = %s,
                                   mensagem = %s
                             WHERE id = %s
                            """,
                            (inicio, fim, status, mensagem, processamento_id),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE processamento
                               SET inicio = %s,
                                   fim = %s,
                                   status = %s
                             WHERE id = %s
                            """,
                            (inicio, fim, status, processamento_id),
                        )
                conn.commit()
        except Exception as e:
            self.logger.error("Erro ao atualizar execução: %s: %s", type(e).__name__, e)

    def registrar_evento(
        self,
        processamento_id: int,
        level: str,
        etapa: Optional[str] = None,
        codigo: Optional[str] = None,
        mensagem: Optional[str] = None,
        detalhe: Optional[dict] = None,
        stacktrace: Optional[str] = None,
    ) -> Optional[int]:
        try:
            lvl = self._norm_level(level)
            detalhe_json = json.dumps(detalhe) if isinstance(detalhe, (dict, list)) else (detalhe if isinstance(detalhe, str) else None)
            with self._get_conn() as conn:
                if conn is None:
                    self.logger.error("Conexão ao banco falhou (log_evento).")
                    return None
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO processamento_log (processamento_id, level, etapa, codigo, mensagem, detalhe, stacktrace)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                        RETURNING id
                        """,
                        (processamento_id, lvl, etapa, codigo, mensagem, detalhe_json, stacktrace),
                    )
                    new_id = cur.fetchone()[0]
                conn.commit()
            return new_id
        except Exception as e:
            self.logger.error("Erro ao registrar evento: %s: %s", type(e).__name__, e)
            return None

    def registrar_execucao(
        self,
        nome_processo: str,
        inicio: Optional[datetime] = None,
        fim: Optional[datetime] = None,
        usuario: Optional[str] = None,
        status: Optional[str] = "SUCCESS",
        mensagem: Optional[str] = None,
    ) -> Optional[int]:
        try:
            self._ensure_schema_introspection()
            now_utc = self._now_utc()
            inicio = inicio or now_utc
            fim = fim or now_utc
            usuario = usuario or self.usuario
            status = (status or "SUCCESS").upper()
            if mensagem is None:
                mensagem = self._buffer_compacto()

            with self._get_conn() as conn:
                if conn is None:
                    self.logger.error("Conexão ao banco falhou.")
                    return None
                with conn.cursor() as cur:
                    if self._has_mensagem_col:
                        cur.execute(
                            """
                            INSERT INTO processamento (nome_processo, inicio, fim, usuario, status, mensagem)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (nome_processo, inicio, fim, usuario, status, mensagem),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO processamento (nome_processo, inicio, fim, usuario, status)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (nome_processo, inicio, fim, usuario, status),
                        )
                    new_id = cur.fetchone()[0]
                conn.commit()
            return new_id
        except Exception as e:
            self.logger.error("Erro ao registrar execucao: %s: %s", type(e).__name__, e)
            return None

    # ---------- Context manager ----------
    @contextmanager
    def execucao(self, nome_processo: str, usuario: Optional[str] = None):
        inicio = self._now_utc()
        proc_id = self.registrar_inicio(nome_processo=nome_processo, usuario=usuario)
        status_final = self._status_success()
        try:
            yield
        except Exception as e:
            status_final = self._status_error()
            tb = traceback.format_exc()
            self.log_mensagem(f"Exceção: {type(e).__name__}: {e}", level="ERROR", etapa="runtime", codigo="UNHANDLED_EXCEPTION")
            self.registrar_evento(
                processamento_id=proc_id or 0,
                level="ERROR",
                etapa="runtime",
                codigo="TRACEBACK",
                mensagem=str(e),
                detalhe=None,
                stacktrace=tb,
            )
            raise
        finally:
            fim = self._now_utc()
            resumo = self._buffer_compacto()
            if proc_id:
                self.atualizar_execucao(
                    processamento_id=proc_id,
                    inicio=inicio,
                    fim=fim,
                    status=status_final,
                    mensagem=resumo,
                )
