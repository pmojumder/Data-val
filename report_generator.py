###############################################################
# HTML REPORT GENERATOR
###############################################################

from datetime import datetime


class HTMLReport:

    def __init__(self, filename="ETL_Report.html"):

        self.filename = filename
        self.results = []

    ###########################################################
    # ADD RESULT
    ###########################################################

    def add_result(self, validation, status, details=""):

        self.results.append({
            "validation": validation,
            "status": status,
            "details": details
        })

    ###########################################################
    # GENERATE HTML REPORT
    ###########################################################

    def generate_report(self):

        total = len(self.results)

        passed = len(
            [r for r in self.results if r["status"] == "PASS"]
        )

        failed = len(
            [r for r in self.results if r["status"] in ["FAIL", "ERROR"]]
        )

        with open(self.filename, "w", encoding="utf-8") as file:

            file.write(f"""
<!DOCTYPE html>

<html>

<head>

<title>ETL Automation Test Report</title>

<style>

body{{
font-family:Arial;
background:#f4f4f4;
margin:30px;
}}

h1{{
background:#003366;
color:white;
padding:15px;
text-align:center;
}}

.summary{{
display:flex;
gap:20px;
margin-bottom:25px;
}}

.card{{
background:white;
padding:15px;
border-radius:8px;
box-shadow:0px 2px 8px gray;
width:220px;
text-align:center;
font-size:18px;
}}

table{{
width:100%;
border-collapse:collapse;
background:white;
}}

th{{
background:#0b5394;
color:white;
padding:10px;
}}

td{{
border:1px solid #ddd;
padding:8px;
}}

.pass{{
color:green;
font-weight:bold;
}}

.fail{{
color:red;
font-weight:bold;
}}

.error{{
color:orange;
font-weight:bold;
}}

</style>

</head>

<body>

<h1>BEVERAGES ETL AUTOMATION TEST REPORT</h1>

<h3>Execution Time : {datetime.now()}</h3>

<div class="summary">

<div class="card">
<b>Total Validations</b><br><br>
{total}
</div>

<div class="card">
<b style="color:green">Passed</b><br><br>
{passed}
</div>

<div class="card">
<b style="color:red">Failed</b><br><br>
{failed}
</div>

</div>

<table>

<tr>

<th>Validation</th>

<th>Status</th>

<th>Details</th>

</tr>
""")

            for result in self.results:

                css = result["status"].lower()

                file.write(f"""

<tr>

<td>{result['validation']}</td>

<td class="{css}">
{result['status']}
</td>

<td>{result['details']}</td>

</tr>

""")

            file.write("""

</table>

<br>

<h3>Pipeline Status :
<span style="color:green">
Completed
</span>
</h3>

</body>

</html>

""")

        print("\n================================================")
        print("HTML REPORT GENERATED SUCCESSFULLY")
        print("Report :", self.filename)
        print("================================================")


###############################################################
# GLOBAL REPORT OBJECT
###############################################################

report = HTMLReport()