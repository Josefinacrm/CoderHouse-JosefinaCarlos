import bcrypt

# Contraseña en texto plano
password_plana = "Wlh1OQ34ir"

# Encriptar 
password_encriptada = bcrypt.hashpw(password_plana.encode('utf-8'), bcrypt.gensalt())

# Imprimir la contraseña encriptada
print(password_encriptada.decode('utf-8'))
