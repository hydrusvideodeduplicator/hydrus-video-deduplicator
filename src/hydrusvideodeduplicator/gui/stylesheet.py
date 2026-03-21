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
