# %%
# %pip install pandas

# %%
base_path = "../../data/"
import pandas as pd

# %%
def clean_data(df):
    pattern_cut = r"[\(,].*$"
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.replace(pattern_cut, "", regex=True).str.strip()
    sufijos = r"\b(MSc|BSc|PhD|MBA|DBA|CCIE|CISSP|PMP|CEH|ENP|FRSA)\b\.?,?"
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.replace(sufijos, "", regex=True, case=False).str.strip()
    emoji_pat = r"[\U00010000-\U0010ffff]"
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.replace(emoji_pat, "", regex=True)
    hyphen_pat = r"(?<=\w)-(?=\w)"
    for col in ["First Name", "Last Name"]:
        df[col] = (
            df[col]
            .str.replace(hyphen_pat, " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    particles = [" de los ", " de la ", " del ", " y "]
    for p in particles:
        for col in ["First Name", "Last Name"]:
            df[col] = df[col].str.replace(p, " ", case=False, regex=False)
    valid_chars = r"[^A-Za-zÁÉÍÓÚÑáéíóúñ\s]"
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.replace(valid_chars, "", regex=True)
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()
    for col in ["First Name", "Last Name"]:
        df[col] = df[col].str.title()
    return df


df = pd.read_csv(base_path + "Connections.csv", encoding="latin-1")

df_clean = clean_data(df.copy())
df_clean.head()

df_clean.to_csv(base_path + "Connections.csv")

# %%
import pandas as pd


def clean_data(df):
    pattern_cut = r"[\(,].*$"
    for col in ["To"]:
        df[col] = df[col].str.replace(pattern_cut, "", regex=True).str.strip()
    sufijos = r"\b(MSc|BSc|PhD|MBA|DBA|CCIE|CISSP|PMP|CEH|ENP|FRSA)\b\.?,?"
    for col in ["To"]:
        df[col] = df[col].str.replace(sufijos, "", regex=True, case=False).str.strip()
    emoji_pat = r"[\U00010000-\U0010ffff]"
    for col in ["To"]:
        df[col] = df[col].str.replace(emoji_pat, "", regex=True)
    hyphen_pat = r"(?<=\w)-(?=\w)"
    for col in ["To"]:
        df[col] = (
            df[col]
            .str.replace(hyphen_pat, " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    particles = [" de los ", " de la ", " del ", " y ", " de "]
    for p in particles:
        for col in ["To"]:
            df[col] = df[col].str.replace(p, " ", case=False, regex=False)
    valid_chars = r"[^A-Za-zÁÉÍÓÚÑáéíóúñ\s]"
    for col in ["To"]:
        df[col] = df[col].str.replace(valid_chars, "", regex=True)
    for col in ["To"]:
        df[col] = df[col].str.replace(r"\s+", " ", regex=True).str.strip()
    for col in ["To"]:
        df[col] = df[col].str.title()
    return df


df = pd.read_csv(base_path + "Invitations.csv", encoding="latin-1")

df_clean = clean_data(df.copy())
df_clean.head()

df_clean.to_csv(base_path + "Invitations.csv")

# %%



