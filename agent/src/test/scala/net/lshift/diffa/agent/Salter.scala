package net.lshift.diffa.agent

import org.springframework.security.authentication.encoding.{MessageDigestPasswordEncoder, ShaPasswordEncoder}


object Salter {

  def main(a:Array[String]) {

    val encoder = new ShaPasswordEncoder(256)
    var valid = encoder.isPasswordValid("84983c60f7daadc1cb8698621f802c0d9f9a3c3c295c810748fb048115c186ec", "guest", null)

    println(valid)

    val pass = encoder.encodePassword("password",null)
    println(pass)

    valid = encoder.isPasswordValid(pass, "password", null)
  }
}
