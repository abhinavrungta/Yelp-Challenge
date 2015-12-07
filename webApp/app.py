from flask import Flask, render_template,json, request, make_response
app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html')

@app.route('/showSignUp')
def showSignUp():
    return render_template('signup.html')

@app.route('/signUp', methods=['GET', 'POST'])
def index():
    errors = []
    results = {}
    if request.method == "POST":
        # get url that the user has entered
        try:
            pincode = request.form['pincode']
            category = request.form['category']
            r = requests.get(url)

        except:
            errors.append(
                "Unable to get URL. Please make sure it's valid and try again."
            )
    data = getData()
    result_html = render_template('results.html', data=json.dumps(data), location = data,pincode = pincode, category = category)
    response= make_response(result_html)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    return response

def getData():
    locations = [
    ['Bondi Beach', -33.890542, 151.274856, 4],
    ['Coogee Beach', -33.923036, 151.259052, 5],
    ['Cronulla Beach', -34.028249, 151.157507, 3],
    ['Manly Beach', -33.80010128657071, 151.28747820854187, 2],
    ['Maroubra Beach', -33.950198, 151.259302, 1],
    ['Bondi Beach', -34.890542, 152.274856, 4],
    ['Coogee Beach', -33.983036, 151.559052, 5],
    ['Cronulla Beach', -34.428249, 151.757507, 3],
    ['Manly Beach', -33.60010128657071, 151.58747820854187, 2],
    ['Maroubra Beach', -33.650198, 151.359302, 1]
    ]
    return locations

if __name__ == "__main__":
    app.run(debug = True)
