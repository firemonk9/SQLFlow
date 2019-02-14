package com.metlife.mask

import java.util.Locale

case class Person(val firstName: String, val lastName: String, val company: String, val prefix: String) {
  def getPrefix: String = prefix

  def getFirstName: String = firstName

  def getLastName: String = lastName

  def getFullName: String = getPrefix + " " + getFirstName + " " + getLastName

  def getCompany: String = company

  def getDomainName: String = getCompany.toLowerCase(Locale.ENGLISH).replaceAll("[^a-zA-Z0-9]*", "") + ".com"

  def getUserName: String = { // Galen Ullrich -> galenullrich, avoid having the string null
    (getFirstName + "." + getLastName).toLowerCase(Locale.ENGLISH).replaceAll("[^a-zA-Z0-9]*", "").replaceAll("null", "n_ll")
  }

  def getEmail: String = getUserName + "@" + getDomainName

  override def toString: String = "Person [ firstName = '" + firstName + "', lastName = '" + lastName + "', userName = '" + getUserName + "', fullName = '" + getFullName + "', company = '" + getCompany + "', domainName = '" + getDomainName + "', userName = '" + getUserName + "', email = '" + getEmail + "' ]"
}
