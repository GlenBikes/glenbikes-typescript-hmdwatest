declare interface String {
  lpad(pad: string, length : number) : string;
}

String.prototype.lpad = function(this: string, pad:string, length: number) {
    var str: string = this;
    while (str.length < length)
        str = pad + str;
    return str;
}

