from flask import jsonify

def setReturn(params):
    # text, httpCode => (text, httpCode) <= TUPLA
    # text           => text <= TEXTO, ARRAY, LISTA, ETC

    # isTuple = type(params).__name__ == "tuple"
    isTuple = isinstance(params, tuple)
    
    httpCode = params[1] if isTuple else 200
    if isTuple: params = params[0]

    return jsonify(params), httpCode