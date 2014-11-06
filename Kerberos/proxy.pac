function FindProxyForURL(url, host)
{
  if (isInNet(host, "192.168.0.0", "255.255.0.0")
      || isInNet(host, "10.0.0.0", "255.0.0.0")
      || host == "localhost"
      || host == "127.0.0.1")
    return "DIRECT";

  else if (shExpMatch(host, "vocms144.cern.ch"))
    return "SOCKS5 127.0.0.1:47171";

  else if (shExpMatch(host, "vocms202.cern.ch"))
    return "SOCKS5 127.0.0.1:47172";

  else if (shExpMatch(host, "vocms201.cern.ch"))
    return "SOCKS5 127.0.0.1:47173";

  else if (shExpMatch(host, "vocms215.cern.ch"))
    return "SOCKS5 127.0.0.1:47174";

  else if (shExpMatch(host, "vocms216.cern.ch"))
    return "SOCKS5 127.0.0.1:47175";

  else if (shExpMatch(host, "vocms85.cern.ch"))
    return "SOCKS5 127.0.0.1:47176";

  else if (shExpMatch(host, "vocms109.cern.ch"))
    return "SOCKS5 127.0.0.1:47177";

  else if (shExpMatch(host, "vocms96.cern.ch"))
    return "SOCKS5 127.0.0.1:47178";

  else if (shExpMatch(host, "vocms15.cern.ch"))
    return "SOCKS5 127.0.0.1:47179";

  else if (shExpMatch(host, "vocms13.cern.ch"))
    return "SOCKS5 127.0.0.1:47180";

  else if (shExpMatch(host, "vocms*.cern.ch")
           || shExpMatch(host, "dmwm*.cern.ch")
           || shExpMatch(host, "reqmon*.cern.ch")
           || shExpMatch(host, "cmstst*.cern.ch")
           || shExpMatch(host, "regsvc*.cern.ch")
           || shExpMatch(host, "samval*.cern.ch")
           || shExpMatch(host, "sryu-*.cern.ch")
           || shExpMatch(host, "ov*.cern.ch")
           || shExpMatch(host, "c2adm01.cern.ch")
           || shExpMatch(host, "cms-logbook-test.cern.ch"))
    return "SOCKS5 127.0.0.1:47170";

  else if (shExpMatch(host, "cmssrv98.fnal.gov"))
    return "SOCKS5 127.0.0.1:47271";

  else if (shExpMatch(host, "cmssrv112.fnal.gov"))
    return "SOCKS5 127.0.0.1:47272";

  else if (shExpMatch(host, "cmssrv73.fnal.gov"))
    return "SOCKS5 127.0.0.1:47273";

  else if (shExpMatch(host, "cmssrv94.fnal.gov"))
    return "SOCKS5 127.0.0.1:47274";

  else if (shExpMatch(host, "cmssrv113.fnal.gov"))
    return "SOCKS5 127.0.0.1:47275";

  else if (shExpMatch(host, "cmssrv101.fnal.gov"))
    return "SOCKS5 127.0.0.1:47276";

  else if (shExpMatch(host, "cmssrv*.fnal.gov")
           || shExpMatch(host, "cmswiki.fnal.gov")
           || shExpMatch(host, "cmslpcweb.fnal.gov")
           || shExpMatch(host, "miscomp.fnal.gov")
           || shExpMatch(host, "appora.fnal.gov")
           || shExpMatch(host, "hrweb.fnal.gov")
           || shExpMatch(host, "bss.fnal.gov"))
    return "SOCKS5 127.0.0.1:47270";

  return "DIRECT";
}
