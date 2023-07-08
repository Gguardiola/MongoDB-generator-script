#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from optparse import OptionParser
import pymongo
from random import randint
from faker import Faker
fake = Faker('es_ES')

num_comptes = 900
num_adreces = 500
num_titulars = 1000
#restamos el num_adreces ya que al asegurarnos 1 titular por cada adreça iterarmos el resto de titulars aleatorios
titulars_restantes = num_titulars - num_adreces
num_contractes = 4000
# restamos el num_comptes ya que al asegurarnos 1 contracte por cada compte iteraremos el resto de contractes de forma aleatoria. Le sumo 1 por un condicional que no quiero cambiar
num_contractes = num_contractes - num_comptes +1
atypes = ('C', 'L', 'S')
vowels = ('a', 'e', 'i', 'o', 'u')
somecons = ('b', 'd', 'f', 'k', 'l', 'm', 'p', 'r', 's')
#total_titulars se rellenará con la generación de los 1000 titulars necesarios para comptes y adreces
total_titulars = []
#used_titulars guarda los ya usados en adreces
used_titulars = []

def r(lim):
  "0 <= random int < lim"
  return randint(0, lim-1)

def randname(syll):
  "random name with syll 2-letter syllables"
  v = len(vowels)
  c = len(somecons)
  res = str()
  for i in range(syll):
    res += somecons[r(c)] + vowels[r(v)]
  return res.capitalize()

def generarTitulars():
  for i in range(num_titulars):
    owner_id = randint(10000000, 99999999)
    owner    = randname(2) + ' ' + randname(4)  
    total_titulars.append({"owner_id": owner_id, "owner": owner})


def create_comptes(db, num_contractes):
  print("Creating comptes collection")
  db.comptes
  db.comptes.delete_many({})
  db.comptes.create_index([('acc_id', pymongo.ASCENDING)], unique=True)
  print("%d comptes will be inserted." % num_comptes)

  for i in range(num_comptes):
    print(i+1, end = '\r')
    acc_id  = randint(100000000000, 999999999999)
    balance = randint(100, 99999)/100
    typ = atypes[r(3)]
    titulars_pos = randint(0,num_titulars -1)
    #introducimos el document en la collection comptes
    try:
      db.comptes.insert_one(
        {"acc_id": acc_id, 
         "type": typ, 
         "balance": balance,
         "titulars": []
         })
      #aseguramos mínimo un contracte
      db.comptes.update_one( { "acc_id" : acc_id }, { "$push" : { "titulars" : total_titulars[titulars_pos] } })
      #ahora introducimos de 3 a 5 contractes adicionales por compte
      contractes_compte = randint(3,5)
      for i in range(contractes_compte):
        if num_contractes != 1:
          titulars_pos = randint(0,num_titulars -1)
          if not db.comptes.find_one({"acc_id": acc_id, "titulars":{"$in" : [total_titulars[titulars_pos]]} }):
            db.comptes.update_one( { "acc_id" : acc_id }, { "$push" : { "titulars" : total_titulars[titulars_pos] } })  
            num_contractes = num_contractes -1
          else:
            i += 1
        else:
          break

    except Exception as e:
      print("ERROR: {}".format(e))


def create_adreces(db,num_titulars, titulars_restantes):
  print("Creating adreces collection")
  db.adreces
  db.adreces.delete_many({})
  db.adreces.create_index([('address', pymongo.ASCENDING)], unique=True)
  db.adreces.create_index([('phone', pymongo.ASCENDING)], unique=True)
  print("%d adreces will be inserted." % num_adreces)
  titulars_pos = randint(0,num_titulars -1)
  for i in range(num_adreces):
    print(i+1, end = '\r')
    address = fake.address()
    phone   = fake.phone_number()
    #introducimos el document en la collection
    try:
      db.adreces.insert_one({"address": address,
                                    "phone": phone,
                                    "titulars": []
      })
      #aseguramos mínimo 1 titular por adreça
      while True:
        if titulars_pos not in used_titulars:
          db.adreces.update_one( { "address" : address }, { "$push" : { "titulars" :{"owner_id": total_titulars[titulars_pos]["owner_id"]} } })
          used_titulars.append(titulars_pos)
          break
        else:
          titulars_pos = randint(0,num_titulars -1)

      #introducimos de 2 a 5 titulars adicionales por cada adreça
      titulars_adreces = randint(2,5)
      for i in range(titulars_adreces):
        if titulars_restantes > 0:
          titulars_pos = randint(0,num_titulars -1)
          if titulars_pos not in used_titulars:
            db.adreces.update_one( { "address" : address }, { "$push" : { "titulars" : {"owner_id": total_titulars[titulars_pos]["owner_id"]}} })
            titulars_restantes = titulars_restantes -1
            used_titulars.append(titulars_pos)
          else:
            i += 1
    except Exception as e:
      print("ERROR: {}".format(e))


# Programa principal
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.bank

generarTitulars()
create_comptes(db, num_contractes)
create_adreces(db,num_titulars,titulars_restantes)


