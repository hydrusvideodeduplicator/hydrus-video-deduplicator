from __future__ import annotations

from typing import TYPE_CHECKING
import sys
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QGridLayout,
    QPushButton,
    QMessageBox,
    QLabel,
    QLineEdit,
)
from PySide6.QtCore import Qt, Signal, QObject, Slot, QThread, QSemaphore
from PySide6.QtGui import QFont
from dataclasses import dataclass
from pathlib import Path

if TYPE_CHECKING:
    from collections.abc import Sequence


from hydrusvideodeduplicator.client import (
    ClientAPIException,
    FailedHVDClientConnection,
    create_client,
)

import logging
from hydrusvideodeduplicator.db import DedupeDB
from hydrusvideodeduplicator.__about__ import __version__
from hydrusvideodeduplicator.config import (
    Config,
)
from hydrusvideodeduplicator.dedup_util import print_and_log
from hydrusvideodeduplicator.dedup import (
    HydrusVideoDeduplicator,
    DedupeProgress,
    NoneProgress,
    HashingProgress,
    BuildingSearchTreeProgress,
    SearchingForDuplicatesProgress,
    DoneProgress,
)

DARK_STYLESHEET = """
QWidget {
    background-color: #1a1a1a;
    color: #e8e8e8;
    font-family: "Segoe UI";
    font-size: 13pt;
}

QLabel {
    color: #e8e8e8;
    font-weight: 450;
}

QLineEdit {
    background-color: #2a2a2a;
    color: #e8e8e8;
    border: 1.5px solid #3a3a3a;
    border-radius: 4px;
    padding: 8px 10px;
    font-size: 12pt;
    selection-background-color: #0d47a1;
}

QLineEdit:focus {
    border: 1.5px solid #4a9eff;
    background-color: #2e2e2e;
}

QLineEdit::placeholder {
    color: #888888;
}

QLineEdit:read-only {
    background-color: #242424;
    color: #a8a8a8;
    border: 1.5px solid #333333;
}

QPushButton {
    background-color: #0d47a1;
    color: #ffffff;
    border: none;
    border-radius: 5px;
    padding: 10px 16px;
    font-weight: 600;
    font-size: 12pt;
}

QPushButton:hover {
    background-color: #1565c0;
}

QPushButton:pressed {
    background-color: #0a3d91;
}

QPushButton:disabled {
    background-color: #2a2a2a;
    color: #737373;
}

QCheckBox {
    spacing: 8px;
    color: #e8e8e8;
}

QCheckBox::indicator {
    width: 16px;
    height: 16px;
}

QCheckBox::indicator:unchecked {
    border: 1.5px solid #555555;
    background-color: #2a2a2a;
    border-radius: 2px;
}

QCheckBox::indicator:checked {
    border: 1.5px solid #4a9eff;
    background-color: #0d47a1;
    border-radius: 2px;
}

QMessageBox {
    background-color: #1a1a1a;
}

QMessageBox QLabel {
    color: #e8e8e8;
}
"""


@dataclass
class HydrusRequestParameters:
    api_url: str
    api_key: str
    local_file_service_keys: list[str]
    requests_ca_bundle: str | None


@dataclass
class DedupeParameters:
    job_count: int
    failed_page_name: str | None
    custom_query: Sequence[str] | None
    debug: bool
    threshold: float
    skip_hashing: bool


@dataclass
class APITestResult:
    dedupe_api_version: str
    hydrus_api_version: str


class Worker(QObject):
    progress = Signal(int)
    dedupe_completed = Signal(str, Exception)
    test_api_connection_completed = Signal(APITestResult, Exception)
    db_stats_completed = Signal(DedupeDB.DatabaseStats)
    reset_hydrus_potential_duplicates_completed = Signal(Exception)
    run_db_maintenance_completed = Signal(Exception, DedupeDB.DatabaseStats, DedupeDB.DatabaseStats)

    progress_updated = Signal(DedupeProgress)

    db_upgrade_started = Signal()
    db_upgrade_completed = Signal(Exception)

    db: DedupeDB.DedupeDb | None = None
    logger: logging.Logger

    @Slot(logging.Logger)
    def init(self, logger: logging.Logger, should_skip_step_semaphore: QSemaphore):
        self.logger = logger
        self.should_skip_step_semaphore = should_skip_step_semaphore

    def update_progress(self, progress: DedupeProgress):
        self.progress_updated.emit(progress)

    def should_skip_step(self) -> bool:
        return self.should_skip_step_semaphore.available() == 0

    @Slot(int)
    def dedupe_connection(self, request_params: HydrusRequestParameters, dedupe_params: DedupeParameters):
        print_and_log(self.logger, f"Connecting to Hydrus at {request_params.api_url}")
        try:
            hvdclient = create_client(
                request_params.local_file_service_keys,
                request_params.api_url,
                request_params.api_key,
                request_params.requests_ca_bundle,
            )
            api_version = hvdclient.get_api_version()
            hydrus_api_version = hvdclient.get_hydrus_api_version()
            print_and_log(self.logger, f"Dedupe API version: 'v{api_version}'")
            print_and_log(self.logger, f"Hydrus API version: 'v{hydrus_api_version}'")
            hvdclient.verify_permissions()

            deduper = HydrusVideoDeduplicator(
                self.db,
                client=hvdclient,
                job_count=dedupe_params.job_count,
                failed_page_name=dedupe_params.failed_page_name,
                custom_query=dedupe_params.custom_query,
                update_progress_callback=self.update_progress,
                should_skip_step_fn=self.should_skip_step,
            )

            if dedupe_params.debug:
                deduper.hydlog.setLevel(logging.DEBUG)
                deduper._DEBUG = True

            if dedupe_params.threshold < 0.0 or dedupe_params.threshold > 100.0:
                print("[red] ERROR: Invalid similarity threshold. Must be between 0 and 100.")
                raise
            HydrusVideoDeduplicator.threshold = dedupe_params.threshold

            num_similar_pairs = deduper.deduplicate(
                skip_hashing=dedupe_params.skip_hashing,
            )
            self.dedupe_completed.emit(f"{num_similar_pairs}", None)
        except (FailedHVDClientConnection, ClientAPIException, Exception) as exc:
            print_and_log(self.logger, str(exc), logging.ERROR)
            # print_and_log(logger, exc.pretty_msg, logging.ERROR)
            self.dedupe_completed.emit(None, exc)

    @Slot()
    def test_api_connection(self, request_params: HydrusRequestParameters):
        # Client connection
        print_and_log(self.logger, f"Connecting to Hydrus at {request_params.api_url}")
        try:
            hvdclient = create_client(
                request_params.local_file_service_keys,
                request_params.api_url,
                request_params.api_key,
                request_params.requests_ca_bundle,
            )
            api_version = hvdclient.get_api_version()
            hydrus_api_version = hvdclient.get_hydrus_api_version()
            print_and_log(self.logger, f"Dedupe API version: 'v{api_version}'")
            print_and_log(self.logger, f"Hydrus API version: 'v{hydrus_api_version}'")
            hvdclient.verify_permissions()
            self.test_api_connection_completed.emit(APITestResult(api_version, hydrus_api_version), None)
        except (FailedHVDClientConnection, ClientAPIException, Exception) as exc:
            print_and_log(self.logger, str(exc), logging.ERROR)
            self.test_api_connection_completed.emit(None, exc)

    @Slot()
    def init_db_connection(self, db_dir: Path):
        if self.db is not None:
            # TODO: Check if transaction is in progress before closing?
            self.db.close()

        # TODO: Show the upgrade on the GUI. Probably pass a callback to db.upgrade_db()
        # for the print msg between upgrades to show an upgrade sequence similar to Hydrus. For
        # the CLI this will be print, for the GUI this will update the GUI msg.
        DedupeDB.set_db_dir(db_dir)
        if DedupeDB.does_db_exist():
            try:
                print_and_log(self.logger, f"Found existing database at '{DedupeDB.get_db_file_path()}'")
                self.db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
                self.db.init_connection()
                # Upgrade the database before doing anything.
                self.db.begin_transaction()
                db_upgraded = False
                if self.db.does_need_upgrade():
                    self.db_upgrade_started.emit()
                with self.db.conn:
                    db_upgraded = self.db.upgrade_db()
                # Vacuum DB after a successful database upgrade. This can reduce space by 1/2 in cases of large
                # db migrations.
                if db_upgraded:
                    print_and_log(
                        self.logger,
                        "Database upgraded, vacuuming to save space.",
                    )
                    db_stats = DedupeDB.get_db_stats(self.db)
                    print_and_log(
                        self.logger,
                        f"Database filesize before vacuum: {db_stats.file_size} bytes.",
                    )
                    self.db.vacuum()
                    db_stats = DedupeDB.get_db_stats(self.db)
                    print_and_log(
                        self.logger,
                        f"Database filesize after vacuum: {db_stats.file_size} bytes.",
                    )
                    self.db_upgrade_completed.emit(None)
                db_stats = DedupeDB.get_db_stats(self.db)

                print_and_log(
                    self.logger,
                    f"Database has {db_stats.num_videos} videos already perceptually hashed.",
                )
                print_and_log(
                    self.logger,
                    f"Database filesize: {db_stats.file_size} bytes.",
                )
            except Exception as exc:
                print_and_log(self.logger, f"Database upgrade failed: {exc}")
                self.db = None
                self.db_upgrade_completed.emit(exc)
        else:
            print_and_log(
                self.logger, f"Database not found. Creating one at '{DedupeDB.get_db_file_path()}'", logging.INFO
            )
            if not DedupeDB.get_db_dir().exists():
                DedupeDB.create_db_dir()
            self.db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
            self.db.init_connection()
            self.db.begin_transaction()
            with self.db.conn:
                self.db.create_tables()
            db_stats = DedupeDB.get_db_stats(self.db)

    @Slot()
    def clear_search_cache_connection(self):
        if self.db is None:
            raise RuntimeError("Search cache connection attempted, but DB was not initialized.")

        print_and_log(self.logger, "Clearing the search cache.")
        self.db.begin_transaction()
        with self.db.conn:
            self.db.clear_search_cache()
        print_and_log(self.logger, "Cleared the search cache.")

    @Slot()
    def clear_search_tree_connection(self):
        if self.db is None:
            raise RuntimeError("Search tree connection attempted, but DB was not initialized.")

        print_and_log(self.logger, "Clearing the search tree.")
        self.db.begin_transaction()
        with self.db.conn:
            self.db.clear_search_tree()
        print_and_log(self.logger, "Cleared the search tree.")

    @Slot()
    def db_stats(self):
        if self.db is None:
            raise RuntimeError("DB stats connection attempted, but DB was not initialized.")
        try:
            db_stats = DedupeDB.get_db_stats(self.db)
            self.db_stats_completed.emit(db_stats)
        except Exception as exc:
            print_and_log(self.logger, f"Failed to get DB stats: {exc}.")

    @Slot()
    def reset_hydrus_potential_duplicates(self, request_params: HydrusRequestParameters):
        print_and_log(self.logger, f"Connecting to Hydrus at {request_params.api_url}")
        try:
            hvdclient = create_client(
                request_params.local_file_service_keys,
                request_params.api_url,
                request_params.api_key,
                request_params.requests_ca_bundle,
            )
            api_version = hvdclient.get_api_version()
            hydrus_api_version = hvdclient.get_hydrus_api_version()
            print_and_log(self.logger, f"Dedupe API version: 'v{api_version}'")
            print_and_log(self.logger, f"Hydrus API version: 'v{hydrus_api_version}'")
            hvdclient.verify_permissions()
            hvdclient.reset_potential_duplicates(
                list(
                    hvdclient.get_video_hashes(
                        [
                            "system:filetype=video, gif, apng",
                            "system:has duration",
                            "system:file service is not currently in trash",
                        ]
                    )
                )
            )
        except (FailedHVDClientConnection, ClientAPIException, Exception) as exc:
            print_and_log(self.logger, str(exc), logging.ERROR)
            self.reset_hydrus_potential_duplicates_completed.emit(exc)
        else:
            self.reset_hydrus_potential_duplicates_completed.emit(None)

    @Slot()
    def run_db_maintenance(self):
        if self.db is None:
            raise RuntimeError("DB maintenance was attempted, but DB was not initialized.")

        try:
            stats_before = DedupeDB.get_db_stats(self.db)
            self.db.vacuum()
            stats_after = DedupeDB.get_db_stats(self.db)
        except Exception as exc:
            self.run_db_maintenance_completed.emit(exc, None, None)
        else:
            self.run_db_maintenance_completed.emit(None, stats_before, stats_after)


class MainWindow(QWidget):
    dedupe_requested = Signal(HydrusRequestParameters, DedupeParameters)
    test_api_connection_requested = Signal(HydrusRequestParameters)
    init_requested = Signal(logging.Logger, QSemaphore)
    init_db_requested = Signal(Path)
    clear_search_tree_requested = Signal()
    clear_search_cache_requested = Signal()
    reset_hydrus_potential_duplicates_requested = Signal(HydrusRequestParameters)
    db_stats_requested = Signal()
    run_db_maintenance_requested = Signal()

    def __init__(self, logger: logging.Logger, config: Config):
        super().__init__()
        self.logger = logger
        self.config = config

        self.setWindowTitle("Hydrus Video Deduplicator")
        self.setFixedSize(900, 950)

        # TODO: Add change db dir button.

        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(28, 28, 28, 28)

        self.should_skip_step_semaphore = QSemaphore(n=1)

        # Version label with enhanced styling
        self.version_label = QLabel(f"Hydrus Video Deduplicator v{__version__}", self)
        self.version_label.setAlignment(Qt.AlignmentFlag.AlignHCenter)
        version_font = QFont("Segoe UI", 14)
        version_font.setWeight(QFont.Weight.Bold)
        self.version_label.setFont(version_font)
        self.version_label.setStyleSheet("color: #4a9eff; margin-bottom: 10px;")

        # Input fields section
        self.required_inputs_label = QLabel("Hydrus Connection")
        required_inputs_font = QFont("Segoe UI", 11)
        required_inputs_font.setWeight(QFont.Weight.Bold)
        self.required_inputs_label.setFont(required_inputs_font)
        self.required_inputs_label.setStyleSheet("color: #4a9eff; margin-top: 8px; margin-bottom: 8px;")

        self.api_key_textbox = QLineEdit(placeholderText="REQUIRED: Hydrus API Key")
        self.api_key_textbox.setToolTip("Hydrus API key.")
        self.api_key_textbox.setText(self.config.hydrus_api_key)
        self.api_key_textbox.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_textbox.setMaximumHeight(38)

        self.api_url_textbox = QLineEdit(self.config.hydrus_api_url, placeholderText="REQUIRED: Hydrus API URL")
        self.api_url_textbox.setToolTip(
            'Hydrus API URL. Ensure the http/https matches the option in your Hydrus client in "manage services -> client api -> use https"'  # noqa: E501
        )
        self.api_url_textbox.setMaximumHeight(38)

        # Main action buttons
        self.test_hydrus_api_access = QPushButton("Test Hydrus API Access")

        self.deduplicate_btn = QPushButton("Run Deduplicator")
        self.deduplicate_btn.setFixedHeight(42)
        self.deduplicate_btn.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        self.deduplicate_btn.clicked.connect(self.dedupe_callback)

        self.skip_progress_btn = QPushButton("Skip Step")
        self.skip_progress_btn.setFixedHeight(42)
        self.skip_progress_btn.clicked.connect(self.skip_progress_callback)

        # Progress display
        self.progress_label = QLabel("Progress: Not Running.")
        progress_font = QFont("Segoe UI", 11)
        progress_font.setWeight(QFont.Weight.Bold)
        self.progress_label.setFont(progress_font)
        progress_stylesheet = """
            QLabel {
                color: #4a9eff;
                background-color: #242424;
                border: 1px solid #3a3a3a;
                border-radius: 4px;
                padding: 12px;
                font-weight: 600;
            }
        """
        self.progress_label.setStyleSheet(progress_stylesheet)

        # Advanced options section
        self.hydrus_query_textbox = QLineEdit(
            placeholderText="OPTIONAL: Hydrus Query (leave empty to deduplicate all videos)"
        )
        self.hydrus_query_textbox.setToolTip("TODO")
        self.hydrus_query_textbox.setMaximumHeight(38)

        self.failed_page_name_textbox = QLineEdit(placeholderText="OPTIONAL: Failed Page Name")
        self.failed_page_name_textbox.setToolTip(
            "Name of the Hydrus page that will be populated with files that fail to be perceptually hashed. You must manually create this in Hydrus."  # noqa: E501
        )
        self.failed_page_name_textbox.setMaximumHeight(38)

        self.job_count_textbox = QLineEdit(
            text="-2", placeholderText="REQUIRED: Number of CPU Threads to use for perceptual hashing"
        )
        self.job_count_textbox.setToolTip(
            "Number of CPU threads to use for perceptual hashing. Default is all but one core.\nYou can use all CPUs/threads on your machine by setting to -1. If you set it to -2, all CPUs but one are used."  # noqa: E501
        )
        self.job_count_textbox.setMaximumHeight(38)

        # Advanced options header
        self.dedupe_config_options_label = QLabel("Advanced Options")
        config_font = QFont("Segoe UI", 11)
        config_font.setWeight(QFont.Weight.Bold)
        self.dedupe_config_options_label.setFont(config_font)
        self.dedupe_config_options_label.setStyleSheet("color: #4a9eff; margin-top: 12px; margin-bottom: 8px;")

        # Database operations section
        self.db_operations_label = QLabel("Database Operations")
        db_ops_font = QFont("Segoe UI", 11)
        db_ops_font.setWeight(QFont.Weight.Bold)
        self.db_operations_label.setFont(db_ops_font)
        self.db_operations_label.setStyleSheet("color: #4a9eff; margin-top: 12px; margin-bottom: 8px;")

        self.db_stats_btn = QPushButton("Database Statistics")
        self.db_stats_btn.setFixedHeight(40)
        self.db_stats_btn.clicked.connect(self.db_stats_callback)

        self.clear_search_cache_btn = QPushButton("Clear Search Cache")
        self.clear_search_cache_btn.setFixedHeight(40)
        self.clear_search_cache_btn.clicked.connect(self.clear_search_cache_callback)

        self.clear_search_tree_btn = QPushButton("Clear Search Tree")
        self.clear_search_tree_btn.setFixedHeight(40)
        self.clear_search_tree_btn.clicked.connect(self.clear_search_tree_callback)

        self.reset_hydrus_potential_duplicates_btn = QPushButton("Reset Potential Duplicates Video Pairs")
        self.reset_hydrus_potential_duplicates_btn.setToolTip(
            "Reset potential duplicates video pairs in Hydrus. This will also clear your local video dedupe search cache."  # noqa: E501
        )
        self.reset_hydrus_potential_duplicates_btn.setFixedHeight(40)
        self.reset_hydrus_potential_duplicates_btn.clicked.connect(self.reset_hydrus_potential_duplicates_btn_callback)

        self.run_db_maintenance_btn = QPushButton("Run Database Maintenance")
        self.run_db_maintenance_btn.setToolTip(
            "Run video dedupe database maintenance, including vacuuming your database to reduce its filesize.\nThis will temporarily require ~2x the disk space of the current db to complete.\nYou can view the db file size in the db stats"  # noqa: E501
        )
        self.run_db_maintenance_btn.setFixedHeight(40)
        self.run_db_maintenance_btn.clicked.connect(self.run_db_maintenance_callback)

        self.test_api_connection_btn = QPushButton("Test API Connection")
        self.test_api_connection_btn.setFixedHeight(40)
        self.test_api_connection_btn.clicked.connect(self.test_api_connection_callback)

        # Database info section
        self.db_database_dir_textbox = QLineEdit(
            text=str(self.config.dedupe_database_dir),
            readOnly=True,
        )
        self.db_database_dir_textbox.setMaximumHeight(38)
        self.job_count_textbox.setToolTip(str(self.config.dedupe_database_dir))

        # About section
        self.about_qt_btn = QPushButton("About Qt")
        self.about_qt_btn.setFixedHeight(40)
        self.about_qt_btn.clicked.connect(self.qt_about_callback)

        # Main layout arrangement
        layout.addWidget(self.version_label)

        # Connection inputs
        layout.addWidget(self.required_inputs_label)
        api_layout = QGridLayout()
        api_layout.setSpacing(10)
        api_layout.addWidget(QLabel("API Key"), 0, 0)
        api_layout.addWidget(self.api_key_textbox, 0, 1)
        api_layout.addWidget(QLabel("API URL"), 1, 0)
        api_layout.addWidget(self.api_url_textbox, 1, 1)
        api_layout.setColumnStretch(1, 1)
        layout.addLayout(api_layout)

        # Main action buttons
        main_buttons_layout = QGridLayout()
        main_buttons_layout.setSpacing(10)
        main_buttons_layout.addWidget(self.deduplicate_btn, 0, 0)
        main_buttons_layout.addWidget(self.skip_progress_btn, 0, 1)
        main_buttons_layout.setColumnStretch(0, 1)
        main_buttons_layout.setColumnStretch(1, 1)
        layout.addLayout(main_buttons_layout)

        # Progress display
        layout.addWidget(self.progress_label)

        # Advanced options
        layout.addWidget(self.dedupe_config_options_label)
        config_layout = QGridLayout()
        config_layout.setSpacing(10)
        config_layout.addWidget(QLabel("Hashing Thread Count"), 0, 0)
        config_layout.addWidget(self.job_count_textbox, 0, 1)
        config_layout.addWidget(QLabel("Failed Page Name"), 1, 0)
        config_layout.addWidget(self.failed_page_name_textbox, 1, 1)
        config_layout.setColumnStretch(1, 1)
        layout.addLayout(config_layout)

        # Database operations
        layout.addWidget(self.db_operations_label)
        db_buttons_layout = QGridLayout()
        db_buttons_layout.setSpacing(10)
        db_buttons_layout.addWidget(self.db_stats_btn, 0, 0)
        db_buttons_layout.addWidget(self.test_api_connection_btn, 0, 1)
        db_buttons_layout.addWidget(self.clear_search_cache_btn, 1, 0)
        db_buttons_layout.addWidget(self.clear_search_tree_btn, 1, 1)
        db_buttons_layout.addWidget(self.reset_hydrus_potential_duplicates_btn, 2, 0, 1, 2)
        db_buttons_layout.addWidget(self.run_db_maintenance_btn, 3, 0, 1, 2)
        db_buttons_layout.setColumnStretch(0, 1)
        db_buttons_layout.setColumnStretch(1, 1)
        layout.addLayout(db_buttons_layout)

        # Database directory info
        db_dir_layout = QGridLayout()
        db_dir_layout.setSpacing(10)
        db_dir_layout.addWidget(QLabel("DB Directory"), 0, 0)
        db_dir_layout.addWidget(self.db_database_dir_textbox, 0, 1)
        db_dir_layout.setColumnStretch(1, 1)
        layout.addLayout(db_dir_layout)

        # About button
        layout.addWidget(self.about_qt_btn)

        layout.addStretch()

        self.worker = Worker()
        self.worker_thread = QThread()

        self.worker.dedupe_completed.connect(self.dedupe_completed_callback)
        self.worker.test_api_connection_completed.connect(self.test_api_connection_completed)
        self.worker.db_stats_completed.connect(self.db_stats_completed)
        self.worker.reset_hydrus_potential_duplicates_completed.connect(
            self.reset_hydrus_potential_duplicates_completed
        )
        self.worker.run_db_maintenance_completed.connect(self.run_db_maintenance_completed)
        self.worker.progress_updated.connect(self.progress_updated_callback)
        self.worker.db_upgrade_started.connect(self.db_upgrade_started_callback)
        self.worker.db_upgrade_completed.connect(self.db_upgrade_completed_callback)

        self.dedupe_requested.connect(self.worker.dedupe_connection)
        self.test_api_connection_requested.connect(self.worker.test_api_connection)
        self.init_requested.connect(self.worker.init)
        self.init_db_requested.connect(self.worker.init_db_connection)
        self.clear_search_cache_requested.connect(self.worker.clear_search_cache_connection)
        self.clear_search_tree_requested.connect(self.worker.clear_search_tree_connection)
        self.db_stats_requested.connect(self.worker.db_stats)
        self.reset_hydrus_potential_duplicates_requested.connect(self.worker.reset_hydrus_potential_duplicates)
        self.run_db_maintenance_requested.connect(self.worker.run_db_maintenance)

        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.start()

        self.current_progress = NoneProgress(None)

        self.db_upgrade_dialog = None

        self.init_requested.emit(self.logger, self.should_skip_step_semaphore)
        self.init_db_requested.emit(self.config.dedupe_database_dir)

    def __del__(self):
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.wait(deadline=5)
            try:
                self.worker_thread.terminate()
            except RuntimeError:
                # Thread may have already been deleted.
                pass

    def progress_updated_callback(self, progress: DedupeProgress):
        self.current_progress = progress
        if not self.skip_progress_btn.isEnabled():
            self.should_skip_step_semaphore.release()
            self.skip_progress_btn.setEnabled(True)
        if isinstance(progress, NoneProgress):
            self.progress_label.setText("Progress: Not running.")
        elif isinstance(progress, HashingProgress):
            percentage = int((progress.complete / progress.total * 100) if progress.total > 0 else 0)
            self.progress_label.setText(
                f"Progress: <b>Perceptually hashing files</b><br/>{progress.complete} / {progress.total} files ({percentage}%)"  # noqa: E501
            )
        elif isinstance(progress, BuildingSearchTreeProgress):
            percentage = int((progress.complete / progress.total * 100) if progress.total > 0 else 0)
            self.progress_label.setText(
                f"Progress: <b>Building search tree</b><br/>{progress.complete} / {progress.total} ({percentage}%)"
            )
        elif isinstance(progress, SearchingForDuplicatesProgress):
            percentage = int((progress.complete / progress.total * 100) if progress.total > 0 else 0)
            self.progress_label.setText(
                f"Progress: <b>Searching for duplicates</b><br/>{progress.complete} / {progress.total} ({percentage}%)"
            )
        elif isinstance(progress, DoneProgress):
            self.progress_label.setText("Progress: <b style='color: #4aff4a;'>Done!</b>")
        else:
            self.progress_label.setText("Unknown progress state.")
            assert False, f"Unknown progress state{type(progress)}"

    def db_upgrade_started_callback(self):
        db_upgrade_dialog = QMessageBox(
            windowTitle="Upgrading database.",
            text="Upgrading database. This may take up to a few minutes, depending on your DB size.",
        )
        db_upgrade_dialog.setStandardButtons(QMessageBox.NoButton)
        db_upgrade_dialog.setWindowFlags(Qt.Window | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
        db_upgrade_dialog.setStyleSheet("""
            QMessageBox {
                background-color: #1a1a1a;
            }
            QMessageBox QLabel {
                color: #e8e8e8;
            }
        """)
        self.db_upgrade_dialog = db_upgrade_dialog
        self.db_upgrade_dialog.show()

    def db_upgrade_completed_callback(self, exc: Exception | None):
        if exc is None:
            self.db_upgrade_dialog.close()
            self.db_upgrade_dialog = None
        else:
            if self.db_upgrade_dialog is None:
                db_upgrade_dialog = QMessageBox(
                    windowTitle="Upgrading database.",
                    text="Upgrading database. This may take up to a few minutes, depending on your DB size.",
                )
                db_upgrade_dialog.setStandardButtons(QMessageBox.NoButton)
                db_upgrade_dialog.setWindowFlags(Qt.Window | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
                db_upgrade_dialog.setStyleSheet("""
                    QMessageBox {
                        background-color: #1a1a1a;
                    }
                    QMessageBox QLabel {
                        color: #e8e8e8;
                    }
                """)
                self.db_upgrade_dialog = db_upgrade_dialog

                self.db_upgrade_dialog.setText(
                    f"An error occurred while upgrading your DB.\nThis should not have happened, but your DB is very likely still intact. If this error was NOT caused by running out of storage space during the migration, please see the Contact section in README.md on the github repo to report this issue.\nError: {exc}"  # noqa: E501
                )
                self.db_upgrade_dialog.show()

            abort_button = self.db_upgrade_dialog.addButton(QMessageBox.Abort)
            abort_button.clicked.connect(lambda: sys.exit(1))

    def dedupe_callback(self):
        self.deduplicate_btn.setEnabled(False)
        request_params = self.get_hydrus_request_params()
        try:
            dedupe_params = self.get_dedupe_params()
        except RuntimeError as exc:
            QMessageBox.warning(
                self,
                "ERROR",
                f"Invalid parameters:\n{exc}",
            )
            self.deduplicate_btn.setEnabled(True)
            return

        if len(request_params.api_key) == 0 or len(request_params.api_url) == 0:
            QMessageBox.warning(
                self,
                "ERROR",
                "You must fill in the Hydrus API URL and Hydrus API key fields first.",
            )
            self.deduplicate_btn.setEnabled(True)
            return

        self.dedupe_requested.emit(request_params, dedupe_params)

    def dedupe_completed_callback(self, dedupe_completed_result: str | None, exc: Exception | None):
        self.deduplicate_btn.setEnabled(True)
        result_msg = (
            f"Deduplication was successful!\nNumber of similar pairs found: {dedupe_completed_result}\nOpen the Hydrus duplicates processing page to process any potential duplicate pairs."  # noqa: E501
            if dedupe_completed_result
            else f"Deduplication failed.\nError: {exc}"
        )
        QMessageBox.information(
            self,
            "Deduplication Result",
            result_msg,
        )

    def test_api_connection_completed(self, api_test_result: APITestResult | None, exc: Exception | None):
        self.test_api_connection_btn.setEnabled(True)
        result_msg = (
            f"API connection was successful!\nHydrus API Version: v{api_test_result.hydrus_api_version}\nDedupe API version: v{api_test_result.dedupe_api_version}"  # noqa: E501
            if api_test_result
            else f"API connection failed.\nError: {exc}"
        )
        QMessageBox.information(
            self,
            "API Test Result",
            result_msg,
        )

    def reset_hydrus_potential_duplicates_completed(self, result: Exception | None):
        self.reset_hydrus_potential_duplicates_btn.setEnabled(True)
        result_msg = (
            "Resetting potential duplicates was successful!"
            if result is None
            else f"Resetting potential duplicates failed.\nError: {result}"
        )
        QMessageBox.information(
            self,
            "Reset Potential Duplicates Result",
            result_msg,
        )
        if result is not None:
            # Failed, don't clear search cache.
            return

        # Clear the search cache after resetting potential duplicates.
        # It doesn't make sense to ever clear the potential duplicates on Hydrus without clearing the local
        # search cache (unless you had another Video Dedupe instance or something like that). So in 99% of cases
        # it makes sense to clear this.
        self.clear_search_cache_requested.emit()

    def stats_to_string(self, db_stats: DedupeDB.DatabaseStats) -> str:
        return f"DB file size: {db_stats.file_size} bytes \nNumber of Perceptually Hashed Videos: {db_stats.num_videos}"

    def run_db_maintenance_completed(
        self,
        result: Exception | None,
        before_stats: DedupeDB.DatabaseStats | None,
        after_stats: DedupeDB.DatabaseStats | None,
    ):
        self.run_db_maintenance_dialog.close()
        self.run_db_maintenance_dialog = None
        self.run_db_maintenance_btn.setEnabled(True)
        result_msg = (
            f"Database maintenance was successful!\n\nBefore Stats:\n{self.stats_to_string(before_stats)}\n\nAfter Stats:\n{self.stats_to_string(after_stats)}"  # noqa: E501
            if result is None
            else f"Database maintenance failed.\nError: {result}"
        )
        QMessageBox.information(
            self,
            "Database Maintenance Result",
            result_msg,
        )

    def clear_search_cache_callback(self):
        confirm_btn = QMessageBox.question(
            self,
            "Confirm Clear Search Cache",
            "Are you sure you want to clear your search cache?",
            buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            defaultButton=QMessageBox.StandardButton.No,
        )

        if confirm_btn == QMessageBox.StandardButton.Yes:
            self.clear_search_cache_requested.emit()

    def clear_search_tree_callback(self):
        confirm_btn = QMessageBox.question(
            self,
            "Confirm Clear Search Tree",
            "Are you sure you want to clear your search tree?",
            buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            defaultButton=QMessageBox.StandardButton.No,
        )

        if confirm_btn == QMessageBox.StandardButton.Yes:
            self.clear_search_tree_requested.emit()

    def db_stats_callback(self):
        self.db_stats_requested.emit()

    def reset_hydrus_potential_duplicates_btn_callback(self):
        request_params = self.get_hydrus_request_params()

        if len(request_params.api_key) == 0 or len(request_params.api_url) == 0:
            QMessageBox.warning(
                self,
                "ERROR",
                "You must fill in the Hydrus API URL and Hydrus API key fields first.",
            )
            return

        confirm_btn = QMessageBox.question(
            self,
            "Confirm Reset Potential Duplicates",
            "Are you sure you want to reset your potential duplicates?\nThis will clear all pairs of videos marked as potential duplicates in Hydrus, and it will also clear your Hydrus Video Deduplicator search cache.",  # noqa: E501
            buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            defaultButton=QMessageBox.StandardButton.No,
        )

        if confirm_btn == QMessageBox.StandardButton.Yes:
            self.reset_hydrus_potential_duplicates_btn.setEnabled(False)
            self.reset_hydrus_potential_duplicates_requested.emit(request_params)

    def db_stats_completed(self, db_stats: DedupeDB.DatabaseStats):
        QMessageBox.information(
            self,
            "Database Statistics",
            f"{self.stats_to_string(db_stats)}",
            buttons=QMessageBox.StandardButton.Ok,
        )

    def test_api_connection_callback(self):
        self.test_api_connection_btn.setEnabled(False)
        request_params = self.get_hydrus_request_params()

        if len(request_params.api_key) == 0 or len(request_params.api_url) == 0:
            QMessageBox.warning(
                self,
                "ERROR",
                "You must fill in the Hydrus API URL and Hydrus API key fields first.",
            )
            self.test_api_connection_btn.setEnabled(True)
            return

        self.test_api_connection_requested.emit(request_params)

    def skip_progress_callback(self):
        if isinstance(self.current_progress, NoneProgress) or isinstance(self.current_progress, DoneProgress):
            return
        if self.skip_progress_btn.isEnabled():
            self.skip_progress_btn.setEnabled(False)
            self.should_skip_step_semaphore.acquire()

    def qt_about_callback(self):
        QMessageBox.aboutQt(
            self,
            "About QT",
        )

    def run_db_maintenance_callback(self):
        confirm_btn = QMessageBox.question(
            self,
            "Confirm Run Database Maintenance",
            "Are you sure you want to run database maintenance?\nThis will temporarily require ~2x the disk space of the current db to complete.\nYou can view the db file size in the db stats.",  # noqa: E501
            buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            defaultButton=QMessageBox.StandardButton.No,
        )

        if confirm_btn == QMessageBox.StandardButton.Yes:
            self.run_db_maintenance_dialog = QMessageBox(
                windowTitle="Running database maintenance.",
                text="Running database maintenance. This may take up to a few minutes, depending on your DB size.",
            )
            self.run_db_maintenance_dialog.setStandardButtons(QMessageBox.NoButton)
            self.run_db_maintenance_dialog.setWindowFlags(Qt.Window | Qt.CustomizeWindowHint | Qt.WindowTitleHint)
            self.run_db_maintenance_dialog.setStyleSheet("""
                QMessageBox {
                    background-color: #1a1a1a;
                }
                QMessageBox QLabel {
                    color: #e8e8e8;
                }
            """)
            self.run_db_maintenance_dialog.show()
            self.run_db_maintenance_btn.setEnabled(False)
            self.run_db_maintenance_requested.emit()

    def get_hydrus_request_params(self) -> HydrusRequestParameters:
        api_url = self.api_url_textbox.text()
        api_key = self.api_key_textbox.text()

        # TODO: Expose local file service keys to GUI
        # TODO: Expose CA BUNDLES to GUI
        return HydrusRequestParameters(
            api_key=api_key,
            api_url=api_url,
            local_file_service_keys=self.config.hydrus_local_file_service_keys,
            requests_ca_bundle=self.config.requests_ca_bundle,
        )

    def get_dedupe_params(self) -> DedupeParameters:
        """
        Get the dedupe parameters from the user GUI.

        Raises RuntimeError with the error with the explanation for the user.
        """
        try:
            job_count = int(self.job_count_textbox.text())
        except ValueError:
            raise RuntimeError(f"Invalid thread count: '{self.job_count_textbox.text()}'. Must be an integer.")

        # TODO: Validate the format of this. This should be a json string (or find a better UX way to input).
        custom_query = self.hydrus_query_textbox.text()
        if len(custom_query) == 0:
            custom_query = None

        failed_page_name = self.failed_page_name_textbox.text()
        if len(failed_page_name) == 0:
            failed_page_name = None

        threshold = 50.0  # TODO: Expose to GUI
        if threshold < 0.0 or threshold > 100.0:
            raise RuntimeError("Invalid similarity threshold. Must be between 0 and 100.")

        return DedupeParameters(
            job_count=job_count,
            failed_page_name=failed_page_name,
            custom_query=custom_query,
            debug=True,  # TODO: Expose to GUI?
            threshold=threshold,
            skip_hashing=False,
        )


def gui_main(config: Config):
    debug = True

    # CLI debug parameter sets log level to info or debug
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG

    logging.basicConfig(format=" %(asctime)s - %(name)s: %(message)s", datefmt="%H:%M:%S", level=loglevel)
    logger = logging.getLogger("main")
    logger.debug("Starting Hydrus Video Deduplicator.")

    app = QApplication(sys.argv)

    # Native Windows widget metrics + dark theme
    app.setStyle("windowsvista")
    app.setFont(QFont("Segoe UI", 12))
    app.setStyleSheet(DARK_STYLESHEET)

    window = MainWindow(logger, config)
    window.show()
    sys.exit(app.exec())
