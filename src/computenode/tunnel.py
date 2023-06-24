import dns.resolver
import dns.message


class OverrideResolver:
    def __init__(self, overrides):
        self._overrides = overrides

    def _build_message(self, qname):
        return dns.message.from_text(
            f"""id 12345
opcode QUERY
rcode NOERROR
flags QR RD RA
;QUESTION
{qname}. IN A
;ANSWER
{qname}. 37478 IN A {self._overrides[qname]}"""
        )

    def resolve(self, *args, **kwargs):
        if "qname" in kwargs:
            qname = kwargs["qname"]
        else:
            qname = args[0]
        if qname in self._overrides:
            return dns.resolver.Answer(
                qname,
                dns.rdatatype.RdataType.A,
                dns.rdataclass.RdataClass.IN,
                self._build_message(qname),
            )
        else:
            return dns.resolver.resolve(qname)


def use_tunnel(at: str, baseurl: str):
    over = OverrideResolver({f"api.{baseurl}": at, "redis.{baseurl}": at})
    dns.resolver.override_system_resolver(over)
