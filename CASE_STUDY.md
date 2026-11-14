# Studiu de caz — Forex Trading Bot

> Varianta completă, cu video de prezentare: **https://iluz.ro/video/proiect/h-mw9gb34f** · [English](https://iluz.ro/video/proiect/h-mw9gb34f?lang=en)

| | |
|---|---|
| **Client** | proiect intern |
| **An** | 2026 |
| **Livrat** | launcher, server, aplicație web, connector MT5, ghid de utilizare, pachete de actualizare cu rollback, video de prezentare |
| **Tehnologii** | Python, FastAPI, SQLite, JavaScript simplu (fără framework) · Claude Code |
| **Realizat de** | [Iluz Studios](https://iluz.ro) — Octavian Ples |

## Brief-ul

A început ca un bot de forex cu MetaTrader 5 și Python: backtesting, tranzacționare pe cont demo, reguli de risc și un panou de control. A crescut repede într-o platformă cu mai multe moduri, roluri de utilizator, tranzacționare automată, setări și lecții pentru începători. Ulterior: tratarea știrilor economice, o opțiune de auto-învățare, două strategii (termen lung și termen scurt) și un ceas al sesiunilor de piață.

## Cum e construit

- **Motor de semnale:** indicatori și un singur manager de risc, folosit identic în backtest și live; backtester conservator.
- **Platformă:** server FastAPI, SQLite, actualizări în timp real, aplicație web RO/EN fără framework.
- **Connector local:** rulează pe calculatorul utilizatorului și plasează ordinele în MetaTrader 5; datele de autentificare la broker nu pleacă de pe acel calculator.
- **Controlul riscului:** limită de risc pe tranzacție, plafon de poziții, limită de pierdere zilnică, verificări de spread și slippage, mod doar-demo, buton de oprire de urgență pentru admin.
- **Calitate:** fiecare versiune verificată cu suite de teste, teste de mutație și review-uri AI rulate în paralel. Din v1.2.0 lucrul se face pe o copie separată, iar actualizările vin cu rollback.
- **Video:** din capturi ale unei copii de test cu date demo inventate, animat din cod, cu muzică originală.

## Rezultate — sincer

- Aplicația rulează pe calculatorul meu, pe cont demo; nu are clienți și nu e serviciu public.
- În datele istorice testate, strategiile **nu au arătat un avantaj după costuri**, iar auto-învățarea nu a schimbat nimic.
- Proiectul e prezentat ca exercițiu de inginerie: arhitectură, controlul riscului, testare și livrare cu rollback.

> Tranzacționarea forex implică un risc ridicat de pierdere. Cifrele din video sunt date demo. Nimic de aici nu este sfat de investiții.

---

### English summary

A forex signal platform: FastAPI + SQLite signal-engine server, a framework-free RO/EN web app, and a local connector that places orders in the user's MetaTrader 5 while keeping broker credentials on their machine. Layered risk controls (per-trade risk, position caps, daily loss limit, spread/slippage checks, demo-only mode, admin kill switch), mutation testing and rollback-able updates. Honest result: on the historical data tested the strategies showed no edge after costs, so it's presented as an engineering exercise, not a money-maker. Full case study: https://iluz.ro/video/proiect/h-mw9gb34f?lang=en
