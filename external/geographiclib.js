/*
 * Geodesic routines from GeographicLib translated to JavaScript.  For
 * more information, see
 * http://geographiclib.sf.net/html/other.html#javascript
 *
 * The algorithms are derived in
 *
 *    Charles F. F. Karney,
 *    Algorithms for geodesics, J. Geodesy 87, 43-55 (2013),
 *    https://dx.doi.org/10.1007/s00190-012-0578-z
 *    Addenda: http://geographiclib.sf.net/geod-addenda.html
 *
 * Copyright (c) Charles Karney (2011-2014) <charles@karney.com> and licensed
 * under the MIT/X11 License.  For more information, see
 * http://geographiclib.sf.net/
 *
 * Inventory of files;
 *   Math.js
 *   Geodesic.js
 *   GeodesicLine.js
 *   PolygonArea.js
 *   DMS.js
 *   Interface.js
 */
// Math.js
var GeographicLib;
if (!GeographicLib) GeographicLib = {};
GeographicLib.Math = {};
GeographicLib.Math.sq = function(x) {
  return x * x;
};
GeographicLib.Math.hypot = function(x, y) {
  x = Math.abs(x);
  y = Math.abs(y);
  var a = Math.max(x, y),
    b = Math.min(x, y) / (a ? a : 1);
  return a * Math.sqrt(1 + b * b);
};
GeographicLib.Math.cbrt = function(x) {
  var y = Math.pow(Math.abs(x), 1 / 3);
  return x < 0 ? -y : y;
};
GeographicLib.Math.log1p = function(x) {
  var
    y = 1 + x,
    z = y - 1;
  return z === 0 ? x : x * Math.log(y) / z;
};
GeographicLib.Math.atanh = function(x) {
  var y = Math.abs(x);
  y = GeographicLib.Math.log1p(2 * y / (1 - y)) / 2;
  return x < 0 ? -y : y;
};
GeographicLib.Math.sum = function(u, v) {
  var
    s = u + v,
    up = s - v,
    vpp = s - up;
  up -= u;
  vpp -= v;
  t = -(up + vpp);
  return {
    s: s,
    t: t
  };
};
GeographicLib.Math.AngNormalize = function(x) {
  return x >= 180 ? x - 360 : (x < -180 ? x + 360 : x);
};
GeographicLib.Math.AngNormalize2 = function(x) {
  return GeographicLib.Math.AngNormalize(x % 360.0);
};
GeographicLib.Math.AngDiff = function(x, y) {
  var
    d = y - x,
    yp = d + x,
    xpp = yp - d;
  yp -= y;
  xpp -= x;
  var t = xpp - yp;
  if ((d - 180) + t > 0)
    d -= 360;
  else if ((d + 180) + t <= 0)
    d += 360;
  return d + t;
};
GeographicLib.Math.epsilon = Math.pow(0.5, 52);
GeographicLib.Math.degree = Math.PI / 180;
GeographicLib.Math.digits = 53;
GeographicLib.Constants = {};
GeographicLib.Constants.WGS84 = {
  a: 6378137,
  f: 1 / 298.257223563
};
GeographicLib.Accumulator = {};
(function() {
  a = GeographicLib.Accumulator;
  var m = GeographicLib.Math;
  a.Accumulator = function(y) {
    this.Set(y);
  };
  a.Accumulator.prototype.Set = function(y) {
    if (!y) y = 0;
    if (y.constructor === a.Accumulator) {
      this._s = y._s;
      this._t = y._t;
    } else {
      this._s = y;
      this._t = 0;
    }
  };
  a.Accumulator.prototype.Add = function(y) {
    var u = m.sum(y, this._t);
    var v = m.sum(u.s, this._s);
    u = u.t;
    this._s = v.s;
    this._t = v.t;
    if (this._s === 0)
      this._s = u;
    else
      this._t += u;
  };
  a.Accumulator.prototype.Sum = function(y) {
    if (!y)
      return this._s;
    else {
      var b = new a.Accumulator(this);
      b.Add(y);
      return b._s;
    }
  };
  a.Accumulator.prototype.Negate = function() {
    this._s *= -1;
    this._t *= -1;
  };
})();
// Geodesic.js
GeographicLib.Geodesic = {};
GeographicLib.GeodesicLine = {};
(function() {
  var m = GeographicLib.Math;
  var g = GeographicLib.Geodesic;
  var l = GeographicLib.GeodesicLine;
  g.GEOGRAPHICLIB_GEODESIC_ORDER = 6;
  g.nA1_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nC1_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nC1p_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nA2_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nC2_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nA3_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nA3x_ = g.nA3_;
  g.nC3_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nC3x_ = (g.nC3_ * (g.nC3_ - 1)) / 2;
  g.nC4_ = g.GEOGRAPHICLIB_GEODESIC_ORDER;
  g.nC4x_ = (g.nC4_ * (g.nC4_ + 1)) / 2;
  g.maxit1_ = 20;
  g.maxit2_ = g.maxit1_ + m.digits + 10;
  g.tiny_ = Math.sqrt(Number.MIN_VALUE);
  g.tol0_ = m.epsilon;
  g.tol1_ = 200 * g.tol0_;
  g.tol2_ = Math.sqrt(g.tol0_);
  g.tolb_ = g.tol0_ * g.tol1_;
  g.xthresh_ = 1000 * g.tol2_;
  g.CAP_NONE = 0;
  g.CAP_C1 = 1 << 0;
  g.CAP_C1p = 1 << 1;
  g.CAP_C2 = 1 << 2;
  g.CAP_C3 = 1 << 3;
  g.CAP_C4 = 1 << 4;
  g.CAP_ALL = 0x1F;
  g.CAP_MASK = g.CAP_ALL;
  g.OUT_ALL = 0x7F80;
  g.OUT_MASK = 0xFF80;
  g.NONE = 0;
  g.LATITUDE = 1 << 7 | g.CAP_NONE;
  g.LONGITUDE = 1 << 8 | g.CAP_C3;
  g.AZIMUTH = 1 << 9 | g.CAP_NONE;
  g.DISTANCE = 1 << 10 | g.CAP_C1;
  g.DISTANCE_IN = 1 << 11 | g.CAP_C1 | g.CAP_C1p;
  g.REDUCEDLENGTH = 1 << 12 | g.CAP_C1 | g.CAP_C2;
  g.GEODESICSCALE = 1 << 13 | g.CAP_C1 | g.CAP_C2;
  g.AREA = 1 << 14 | g.CAP_C4;
  g.LONG_NOWRAP = 1 << 15;
  g.ALL = g.OUT_ALL | g.CAP_ALL;
  g.SinCosSeries = function(sinp, sinx, cosx, c, n) {
    var k = n + (sinp ? 1 : 0);
    var
      ar = 2 * (cosx - sinx) * (cosx + sinx),
      y0 = n & 1 ? c[--k] : 0,
      y1 = 0;
    n = Math.floor(n / 2);
    while (n--) {
      y1 = ar * y0 - y1 + c[--k];
      y0 = ar * y1 - y0 + c[--k];
    }
    return (sinp ? 2 * sinx * cosx * y0 :
      cosx * (y0 - y1));
  };
  g.AngRound = function(x) {
    var z = 1 / 16;
    var y = Math.abs(x);
    y = y < z ? z - (z - y) : y;
    return x < 0 ? -y : y;
  };
  g.Astroid = function(x, y) {
    var k;
    var
      p = m.sq(x),
      q = m.sq(y),
      r = (p + q - 1) / 6;
    if (!(q === 0 && r <= 0)) {
      var
        S = p * q / 4,
        r2 = m.sq(r),
        r3 = r * r2,
        disc = S * (S + 2 * r3);
      var u = r;
      if (disc >= 0) {
        var T3 = S + r3;
        T3 += T3 < 0 ? -Math.sqrt(disc) :
          Math.sqrt(disc);
        var T = m.cbrt(T3);
        u += T + (T !== 0 ? r2 / T : 0);
      } else {
        var ang = Math.atan2(Math.sqrt(-disc), -(S + r3));
        u += 2 * r * Math.cos(ang / 3);
      }
      var
        v = Math.sqrt(m.sq(u) + q),
        uv = u < 0 ? q / (v - u) : u + v,
        w = (uv - q) / (2 * v);
      k = uv / (Math.sqrt(uv + m.sq(w)) + w);
    } else {
      k = 0;
    }
    return k;
  };
  g.A1m1f = function(eps) {
    var
      eps2 = m.sq(eps),
      t = eps2 * (eps2 * (eps2 + 4) + 64) / 256;
    return (t + eps) / (1 - eps);
  };
  g.C1f = function(eps, c) {
    var
      eps2 = m.sq(eps),
      d = eps;
    c[1] = d * ((6 - eps2) * eps2 - 16) / 32;
    d *= eps;
    c[2] = d * ((64 - 9 * eps2) * eps2 - 128) / 2048;
    d *= eps;
    c[3] = d * (9 * eps2 - 16) / 768;
    d *= eps;
    c[4] = d * (3 * eps2 - 5) / 512;
    d *= eps;
    c[5] = -7 * d / 1280;
    d *= eps;
    c[6] = -7 * d / 2048;
  };
  g.C1pf = function(eps, c) {
    var
      eps2 = m.sq(eps),
      d = eps;
    c[1] = d * (eps2 * (205 * eps2 - 432) + 768) / 1536;
    d *= eps;
    c[2] = d * (eps2 * (4005 * eps2 - 4736) + 3840) / 12288;
    d *= eps;
    c[3] = d * (116 - 225 * eps2) / 384;
    d *= eps;
    c[4] = d * (2695 - 7173 * eps2) / 7680;
    d *= eps;
    c[5] = 3467 * d / 7680;
    d *= eps;
    c[6] = 38081 * d / 61440;
  };
  g.A2m1f = function(eps) {
    var
      eps2 = m.sq(eps),
      t = eps2 * (eps2 * (25 * eps2 + 36) + 64) / 256;
    return t * (1 - eps) - eps;
  };
  g.C2f = function(eps, c) {
    var
      eps2 = m.sq(eps),
      d = eps;
    c[1] = d * (eps2 * (eps2 + 2) + 16) / 32;
    d *= eps;
    c[2] = d * (eps2 * (35 * eps2 + 64) + 384) / 2048;
    d *= eps;
    c[3] = d * (15 * eps2 + 80) / 768;
    d *= eps;
    c[4] = d * (7 * eps2 + 35) / 512;
    d *= eps;
    c[5] = 63 * d / 1280;
    d *= eps;
    c[6] = 77 * d / 2048;
  };
  g.Geodesic = function(a, f) {
    this._a = a;
    this._f = f <= 1 ? f : 1 / f;
    this._f1 = 1 - this._f;
    this._e2 = this._f * (2 - this._f);
    this._ep2 = this._e2 / m.sq(this._f1);
    this._n = this._f / (2 - this._f);
    this._b = this._a * this._f1;
    this._c2 = (m.sq(this._a) + m.sq(this._b) *
      (this._e2 === 0 ? 1 :
        (this._e2 > 0 ? m.atanh(Math.sqrt(this._e2)) :
          Math.atan(Math.sqrt(-this._e2))) /
        Math.sqrt(Math.abs(this._e2)))) / 2;
    this._etol2 = 0.1 * g.tol2_ /
      Math.sqrt(Math.max(0.001, Math.abs(this._f)) *
        Math.min(1.0, 1 - this._f / 2) / 2);
    if (!(isFinite(this._a) && this._a > 0))
      throw new Error("Major radius is not positive");
    if (!(isFinite(this._b) && this._b > 0))
      throw new Error("Minor radius is not positive");
    this._A3x = new Array(g.nA3x_);
    this._C3x = new Array(g.nC3x_);
    this._C4x = new Array(g.nC4x_);
    this.A3coeff();
    this.C3coeff();
    this.C4coeff();
  };
  g.Geodesic.prototype.A3coeff = function() {
    var _n = this._n;
    this._A3x[0] = 1;
    this._A3x[1] = (_n - 1) / 2;
    this._A3x[2] = (_n * (3 * _n - 1) - 2) / 8;
    this._A3x[3] = ((-_n - 3) * _n - 1) / 16;
    this._A3x[4] = (-2 * _n - 3) / 64;
    this._A3x[5] = -3 / 128;
  };
  g.Geodesic.prototype.C3coeff = function() {
    var _n = this._n;
    this._C3x[0] = (1 - _n) / 4;
    this._C3x[1] = (1 - _n * _n) / 8;
    this._C3x[2] = ((3 - _n) * _n + 3) / 64;
    this._C3x[3] = (2 * _n + 5) / 128;
    this._C3x[4] = 3 / 128;
    this._C3x[5] = ((_n - 3) * _n + 2) / 32;
    this._C3x[6] = ((-3 * _n - 2) * _n + 3) / 64;
    this._C3x[7] = (_n + 3) / 128;
    this._C3x[8] = 5 / 256;
    this._C3x[9] = (_n * (5 * _n - 9) + 5) / 192;
    this._C3x[10] = (9 - 10 * _n) / 384;
    this._C3x[11] = 7 / 512;
    this._C3x[12] = (7 - 14 * _n) / 512;
    this._C3x[13] = 7 / 512;
    this._C3x[14] = 21 / 2560;
  };
  g.Geodesic.prototype.C4coeff = function() {
    var _n = this._n;
    this._C4x[0] = (_n * (_n * (_n * (_n * (100 * _n + 208) + 572) + 3432) - 12012) + 30030) / 45045;
    this._C4x[1] = (_n * (_n * (_n * (64 * _n + 624) - 4576) + 6864) - 3003) / 15015;
    this._C4x[2] = (_n * ((14144 - 10656 * _n) * _n - 4576) - 858) / 45045;
    this._C4x[3] = ((-224 * _n - 4784) * _n + 1573) / 45045;
    this._C4x[4] = (1088 * _n + 156) / 45045;
    this._C4x[5] = 97 / 15015.0;
    this._C4x[6] = (_n * (_n * ((-64 * _n - 624) * _n + 4576) - 6864) + 3003) / 135135;
    this._C4x[7] = (_n * (_n * (5952 * _n - 11648) + 9152) - 2574) / 135135;
    this._C4x[8] = (_n * (5792 * _n + 1040) - 1287) / 135135;
    this._C4x[9] = (468 - 2944 * _n) / 135135;
    this._C4x[10] = 1 / 9009.0;
    this._C4x[11] = (_n * ((4160 - 1440 * _n) * _n - 4576) + 1716) / 225225;
    this._C4x[12] = ((4992 - 8448 * _n) * _n - 1144) / 225225;
    this._C4x[13] = (1856 * _n - 936) / 225225;
    this._C4x[14] = 8 / 10725.0;
    this._C4x[15] = (_n * (3584 * _n - 3328) + 1144) / 315315;
    this._C4x[16] = (1024 * _n - 208) / 105105;
    this._C4x[17] = -136 / 63063.0;
    this._C4x[18] = (832 - 2560 * _n) / 405405;
    this._C4x[19] = -128 / 135135.0;
    this._C4x[20] = 128 / 99099.0;
  };
  g.Geodesic.prototype.A3f = function(eps) {
    var v = 0;
    for (var i = g.nA3x_; i;)
      v = eps * v + this._A3x[--i];
    return v;
  };
  g.Geodesic.prototype.C3f = function(eps, c) {
    var j, k;
    for (j = g.nC3x_, k = g.nC3_ - 1; k;) {
      var t = 0;
      for (var i = g.nC3_ - k; i; --i)
        t = eps * t + this._C3x[--j];
      c[k--] = t;
    }
    var mult = 1;
    for (k = 1; k < g.nC3_;) {
      mult *= eps;
      c[k++] *= mult;
    }
  };
  g.Geodesic.prototype.C4f = function(eps, c) {
    var j, k;
    for (j = g.nC4x_, k = g.nC4_; k;) {
      var t = 0;
      for (var i = g.nC4_ - k + 1; i; --i)
        t = eps * t + this._C4x[--j];
      c[--k] = t;
    }
    var mult = 1;
    for (k = 1; k < g.nC4_;) {
      mult *= eps;
      c[k++] *= mult;
    }
  };
  g.Geodesic.prototype.Lengths = function(eps, sig12,
    ssig1, csig1, dn1, ssig2, csig2, dn2,
    cbet1, cbet2, scalep,
    C1a, C2a) {
    var vals = {};
    g.C1f(eps, C1a);
    g.C2f(eps, C2a);
    var
      A1m1 = g.A1m1f(eps),
      AB1 = (1 + A1m1) * (g.SinCosSeries(true, ssig2, csig2, C1a, g.nC1_) -
        g.SinCosSeries(true, ssig1, csig1, C1a, g.nC1_)),
      A2m1 = g.A2m1f(eps),
      AB2 = (1 + A2m1) * (g.SinCosSeries(true, ssig2, csig2, C2a, g.nC2_) -
        g.SinCosSeries(true, ssig1, csig1, C2a, g.nC2_));
    vals.m0 = A1m1 - A2m1;
    var J12 = vals.m0 * sig12 + (AB1 - AB2);
    vals.m12b = dn2 * (csig1 * ssig2) - dn1 * (ssig1 * csig2) -
      csig1 * csig2 * J12;
    vals.s12b = (1 + A1m1) * sig12 + AB1;
    if (scalep) {
      var csig12 = csig1 * csig2 + ssig1 * ssig2;
      var t = this._ep2 * (cbet1 - cbet2) * (cbet1 + cbet2) / (dn1 + dn2);
      vals.M12 = csig12 + (t * ssig2 - csig2 * J12) * ssig1 / dn1;
      vals.M21 = csig12 - (t * ssig1 - csig1 * J12) * ssig2 / dn2;
    }
    return vals;
  };
  g.Geodesic.prototype.InverseStart = function(sbet1, cbet1, dn1,
    sbet2, cbet2, dn2, lam12,
    C1a, C2a) {
    var
      vals = {},
      sbet12 = sbet2 * cbet1 - cbet2 * sbet1,
      cbet12 = cbet2 * cbet1 + sbet2 * sbet1;
    vals.sig12 = -1;
    var sbet12a = sbet2 * cbet1;
    sbet12a += cbet2 * sbet1;
    var shortline = cbet12 >= 0 && sbet12 < 0.5 && cbet2 * lam12 < 0.5;
    var omg12 = lam12;
    if (shortline) {
      var sbetm2 = m.sq(sbet1 + sbet2);
      sbetm2 /= sbetm2 + m.sq(cbet1 + cbet2);
      vals.dnm = Math.sqrt(1 + this._ep2 * sbetm2);
      omg12 /= this._f1 * vals.dnm;
    }
    var somg12 = Math.sin(omg12),
      comg12 = Math.cos(omg12);
    vals.salp1 = cbet2 * somg12;
    vals.calp1 = comg12 >= 0 ?
      sbet12 + cbet2 * sbet1 * m.sq(somg12) / (1 + comg12) :
      sbet12a - cbet2 * sbet1 * m.sq(somg12) / (1 - comg12);
    var t,
      ssig12 = m.hypot(vals.salp1, vals.calp1),
      csig12 = sbet1 * sbet2 + cbet1 * cbet2 * comg12;
    if (shortline && ssig12 < this._etol2) {
      vals.salp2 = cbet1 * somg12;
      vals.calp2 = sbet12 - cbet1 * sbet2 *
        (comg12 >= 0 ? m.sq(somg12) / (1 + comg12) : 1 - comg12);
      t = m.hypot(vals.salp2, vals.calp2);
      vals.salp2 /= t;
      vals.calp2 /= t;
      vals.sig12 = Math.atan2(ssig12, csig12);
    } else if (Math.abs(this._n) > 0.1 ||
      csig12 >= 0 ||
      ssig12 >= 6 * Math.abs(this._n) * Math.PI * m.sq(cbet1)) {
      0;
    } else {
      var y, lamscale, betscale;
      var x;
      if (this._f >= 0) {
        var
          k2 = m.sq(sbet1) * this._ep2,
          eps = k2 / (2 * (1 + Math.sqrt(1 + k2)) + k2);
        lamscale = this._f * cbet1 * this.A3f(eps) * Math.PI;
        betscale = lamscale * cbet1;
        x = (lam12 - Math.PI) / lamscale;
        y = sbet12a / betscale;
      } else {
        var
          cbet12a = cbet2 * cbet1 - sbet2 * sbet1,
          bet12a = Math.atan2(sbet12a, cbet12a);
        var m12b, m0;
        var nvals = this.Lengths(this._n, Math.PI + bet12a,
          sbet1, -cbet1, dn1, sbet2, cbet2, dn2,
          cbet1, cbet2, false, C1a, C2a);
        m12b = nvals.m12b;
        m0 = nvals.m0;
        x = -1 + m12b / (cbet1 * cbet2 * m0 * Math.PI);
        betscale = x < -0.01 ? sbet12a / x :
          -this._f * m.sq(cbet1) * Math.PI;
        lamscale = betscale / cbet1;
        y = (lam12 - Math.PI) / lamscale;
      }
      if (y > -g.tol1_ && x > -1 - g.xthresh_) {
        if (this._f >= 0) {
          vals.salp1 = Math.min(1, -x);
          vals.calp1 = -Math.sqrt(1 - m.sq(vals.salp1));
        } else {
          vals.calp1 = Math.max(x > -g.tol1_ ? 0 : -1, x);
          vals.salp1 = Math.sqrt(1 - m.sq(vals.calp1));
        }
      } else {
        var k = g.Astroid(x, y);
        var
          omg12a = lamscale * (this._f >= 0 ? -x * k / (1 + k) : -y * (1 + k) / k);
        somg12 = Math.sin(omg12a);
        comg12 = -Math.cos(omg12a);
        vals.salp1 = cbet2 * somg12;
        vals.calp1 = sbet12a -
          cbet2 * sbet1 * m.sq(somg12) / (1 - comg12);
      }
    }
    if (!(vals.salp1 <= 0)) {
      t = m.hypot(vals.salp1, vals.calp1);
      vals.salp1 /= t;
      vals.calp1 /= t;
    } else {
      vals.salp1 = 1;
      vals.calp1 = 0;
    }
    return vals;
  };
  g.Geodesic.prototype.Lambda12 = function(sbet1, cbet1, dn1, sbet2, cbet2, dn2,
    salp1, calp1, diffp,
    C1a, C2a, C3a) {
    var vals = {};
    if (sbet1 === 0 && calp1 === 0)
      calp1 = -g.tiny_;
    var t,
      salp0 = salp1 * cbet1,
      calp0 = m.hypot(calp1, salp1 * sbet1);
    var somg1, comg1, somg2, comg2, omg12;
    vals.ssig1 = sbet1;
    somg1 = salp0 * sbet1;
    vals.csig1 = comg1 = calp1 * cbet1;
    t = m.hypot(vals.ssig1, vals.csig1);
    vals.ssig1 /= t;
    vals.csig1 /= t;
    vals.salp2 = cbet2 !== cbet1 ? salp0 / cbet2 : salp1;
    vals.calp2 = cbet2 !== cbet1 || Math.abs(sbet2) !== -sbet1 ?
      Math.sqrt(m.sq(calp1 * cbet1) + (cbet1 < -sbet1 ?
        (cbet2 - cbet1) * (cbet1 + cbet2) :
        (sbet1 - sbet2) * (sbet1 + sbet2))) /
      cbet2 : Math.abs(calp1);
    vals.ssig2 = sbet2;
    somg2 = salp0 * sbet2;
    vals.csig2 = comg2 = vals.calp2 * cbet2;
    t = m.hypot(vals.ssig2, vals.csig2);
    vals.ssig2 /= t;
    vals.csig2 /= t;
    vals.sig12 = Math.atan2(Math.max(vals.csig1 * vals.ssig2 -
        vals.ssig1 * vals.csig2, 0),
      vals.csig1 * vals.csig2 + vals.ssig1 * vals.ssig2);
    omg12 = Math.atan2(Math.max(comg1 * somg2 - somg1 * comg2, 0),
      comg1 * comg2 + somg1 * somg2);
    var B312, h0;
    var k2 = m.sq(calp0) * this._ep2;
    vals.eps = k2 / (2 * (1 + Math.sqrt(1 + k2)) + k2);
    this.C3f(vals.eps, C3a);
    B312 = (g.SinCosSeries(true, vals.ssig2, vals.csig2, C3a, g.nC3_ - 1) -
      g.SinCosSeries(true, vals.ssig1, vals.csig1, C3a, g.nC3_ - 1));
    h0 = -this._f * this.A3f(vals.eps);
    vals.domg12 = salp0 * h0 * (vals.sig12 + B312);
    vals.lam12 = omg12 + vals.domg12;
    if (diffp) {
      if (vals.calp2 === 0)
        vals.dlam12 = -2 * this._f1 * dn1 / sbet1;
      else {
        var nvals = this.Lengths(vals.eps, vals.sig12,
          vals.ssig1, vals.csig1, dn1,
          vals.ssig2, vals.csig2, dn2,
          cbet1, cbet2, false, C1a, C2a);
        vals.dlam12 = nvals.m12b;
        vals.dlam12 *= this._f1 / (vals.calp2 * cbet2);
      }
    }
    return vals;
  };
  g.Geodesic.prototype.GenInverse = function(lat1, lon1, lat2, lon2, outmask) {
    var vals = {};
    outmask &= g.OUT_MASK;
    var lon12 = m.AngDiff(m.AngNormalize(lon1), m.AngNormalize(lon2));
    lon12 = g.AngRound(lon12);
    var lonsign = lon12 >= 0 ? 1 : -1;
    lon12 *= lonsign;
    lat1 = g.AngRound(lat1);
    lat2 = g.AngRound(lat2);
    var t, swapp = Math.abs(lat1) >= Math.abs(lat2) ? 1 : -1;
    if (swapp < 0) {
      lonsign *= -1;
      t = lat1;
      lat1 = lat2;
      lat2 = t;
    }
    var latsign = lat1 < 0 ? 1 : -1;
    lat1 *= latsign;
    lat2 *= latsign;
    var phi, sbet1, cbet1, sbet2, cbet2, s12x, m12x;
    phi = lat1 * m.degree;
    sbet1 = this._f1 * Math.sin(phi);
    cbet1 = lat1 === -90 ? g.tiny_ : Math.cos(phi);
    t = m.hypot(sbet1, cbet1);
    sbet1 /= t;
    cbet1 /= t;
    phi = lat2 * m.degree;
    sbet2 = this._f1 * Math.sin(phi);
    cbet2 = Math.abs(lat2) === 90 ? g.tiny_ : Math.cos(phi);
    t = m.hypot(sbet2, cbet2);
    sbet2 /= t;
    cbet2 /= t;
    if (cbet1 < -sbet1) {
      if (cbet2 === cbet1)
        sbet2 = sbet2 < 0 ? sbet1 : -sbet1;
    } else {
      if (Math.abs(sbet2) === -sbet1)
        cbet2 = cbet1;
    }
    var
      dn1 = Math.sqrt(1 + this._ep2 * m.sq(sbet1)),
      dn2 = Math.sqrt(1 + this._ep2 * m.sq(sbet2));
    var
      lam12 = lon12 * m.degree,
      slam12 = lon12 === 180 ? 0 : Math.sin(lam12),
      clam12 = Math.cos(lam12);
    var sig12, calp1, salp1, calp2, salp2;
    var
      C1a = new Array(g.nC1_ + 1),
      C2a = new Array(g.nC2_ + 1),
      C3a = new Array(g.nC3_);
    var meridian = lat1 === -90 || slam12 === 0,
      nvals;
    var ssig1, csig1, ssig2, csig2, eps;
    if (meridian) {
      calp1 = clam12;
      salp1 = slam12;
      calp2 = 1;
      salp2 = 0;
      ssig1 = sbet1;
      csig1 = calp1 * cbet1;
      ssig2 = sbet2;
      csig2 = calp2 * cbet2;
      sig12 = Math.atan2(Math.max(csig1 * ssig2 - ssig1 * csig2, 0),
        csig1 * csig2 + ssig1 * ssig2);
      nvals = this.Lengths(this._n, sig12,
        ssig1, csig1, dn1, ssig2, csig2, dn2,
        cbet1, cbet2, (outmask & g.GEODESICSCALE) !== 0,
        C1a, C2a);
      s12x = nvals.s12b;
      m12x = nvals.m12b;
      if ((outmask & g.GEODESICSCALE) !== 0) {
        vals.M12 = nvals.M12;
        vals.M21 = nvals.M21;
      }
      if (sig12 < 1 || m12x >= 0) {
        m12x *= this._b;
        s12x *= this._b;
        vals.a12 = sig12 / m.degree;
      } else
        meridian = false;
    }
    var omg12;
    if (!meridian &&
      sbet1 === 0 &&
      (this._f <= 0 || lam12 <= Math.PI - this._f * Math.PI)) {
      calp1 = calp2 = 0;
      salp1 = salp2 = 1;
      s12x = this._a * lam12;
      sig12 = omg12 = lam12 / this._f1;
      m12x = this._b * Math.sin(sig12);
      if (outmask & g.GEODESICSCALE)
        vals.M12 = vals.M21 = Math.cos(sig12);
      vals.a12 = lon12 / this._f1;
    } else if (!meridian) {
      nvals = this.InverseStart(sbet1, cbet1, dn1, sbet2, cbet2, dn2, lam12,
        C1a, C2a);
      sig12 = nvals.sig12;
      salp1 = nvals.salp1;
      calp1 = nvals.calp1;
      if (sig12 >= 0) {
        salp2 = nvals.salp2;
        calp2 = nvals.calp2;
        var dnm = nvals.dnm;
        s12x = sig12 * this._b * dnm;
        m12x = m.sq(dnm) * this._b * Math.sin(sig12 / dnm);
        if (outmask & g.GEODESICSCALE)
          vals.M12 = vals.M21 = Math.cos(sig12 / dnm);
        vals.a12 = sig12 / m.degree;
        omg12 = lam12 / (this._f1 * dnm);
      } else {
        var numit = 0;
        var salp1a = g.tiny_,
          calp1a = 1,
          salp1b = g.tiny_,
          calp1b = -1;
        for (var tripn = false, tripb = false; numit < g.maxit2_; ++numit) {
          var dv;
          nvals = this.Lambda12(sbet1, cbet1, dn1, sbet2, cbet2, dn2,
            salp1, calp1, numit < g.maxit1_,
            C1a, C2a, C3a);
          var v = nvals.lam12 - lam12;
          salp2 = nvals.salp2;
          calp2 = nvals.calp2;
          sig12 = nvals.sig12;
          ssig1 = nvals.ssig1;
          csig1 = nvals.csig1;
          ssig2 = nvals.ssig2;
          csig2 = nvals.csig2;
          eps = nvals.eps;
          omg12 = nvals.domg12;
          dv = nvals.dlam12;
          if (tripb || !(Math.abs(v) >= (tripn ? 8 : 2) * g.tol0_))
            break;
          if (v > 0 && (numit < g.maxit1_ || calp1 / salp1 > calp1b / salp1b)) {
            salp1b = salp1;
            calp1b = calp1;
          } else if (v < 0 &&
            (numit < g.maxit1_ || calp1 / salp1 < calp1a / salp1a)) {
            salp1a = salp1;
            calp1a = calp1;
          }
          if (numit < g.maxit1_ && dv > 0) {
            var
              dalp1 = -v / dv;
            var
              sdalp1 = Math.sin(dalp1),
              cdalp1 = Math.cos(dalp1),
              nsalp1 = salp1 * cdalp1 + calp1 * sdalp1;
            if (nsalp1 > 0 && Math.abs(dalp1) < Math.PI) {
              calp1 = calp1 * cdalp1 - salp1 * sdalp1;
              salp1 = Math.max(0, nsalp1);
              t = m.hypot(salp1, calp1);
              salp1 /= t;
              calp1 /= t;
              tripn = Math.abs(v) <= 16 * g.tol0_;
              continue;
            }
          }
          salp1 = (salp1a + salp1b) / 2;
          calp1 = (calp1a + calp1b) / 2;
          t = m.hypot(salp1, calp1);
          salp1 /= t;
          calp1 /= t;
          tripn = false;
          tripb = (Math.abs(salp1a - salp1) + (calp1a - calp1) < g.tolb_ ||
            Math.abs(salp1 - salp1b) + (calp1 - calp1b) < g.tolb_);
        }
        nvals = this.Lengths(eps, sig12,
          ssig1, csig1, dn1, ssig2, csig2, dn2,
          cbet1, cbet2, (outmask & g.GEODESICSCALE) !== 0,
          C1a, C2a);
        s12x = nvals.s12b;
        m12x = nvals.m12b;
        if ((outmask & g.GEODESICSCALE) !== 0) {
          vals.M12 = nvals.M12;
          vals.M21 = nvals.M21;
        }
        m12x *= this._b;
        s12x *= this._b;
        vals.a12 = sig12 / m.degree;
        omg12 = lam12 - omg12;
      }
    }
    if (outmask & g.DISTANCE)
      vals.s12 = 0 + s12x;
    if (outmask & g.REDUCEDLENGTH)
      vals.m12 = 0 + m12x;
    if (outmask & g.AREA) {
      var
        salp0 = salp1 * cbet1,
        calp0 = m.hypot(calp1, salp1 * sbet1);
      var alp12;
      if (calp0 !== 0 && salp0 !== 0) {
        ssig1 = sbet1;
        csig1 = calp1 * cbet1;
        ssig2 = sbet2;
        csig2 = calp2 * cbet2;
        var k2 = m.sq(calp0) * this._ep2;
        eps = k2 / (2 * (1 + Math.sqrt(1 + k2)) + k2);
        A4 = m.sq(this._a) * calp0 * salp0 * this._e2;
        t = m.hypot(ssig1, csig1);
        ssig1 /= t;
        csig1 /= t;
        t = m.hypot(ssig2, csig2);
        ssig2 /= t;
        csig2 /= t;
        var C4a = new Array(g.nC4_);
        this.C4f(eps, C4a);
        var
          B41 = g.SinCosSeries(false, ssig1, csig1, C4a, g.nC4_),
          B42 = g.SinCosSeries(false, ssig2, csig2, C4a, g.nC4_);
        vals.S12 = A4 * (B42 - B41);
      } else
        vals.S12 = 0;
      if (!meridian &&
        omg12 < 0.75 * Math.PI &&
        sbet2 - sbet1 < 1.75) {
        var
          somg12 = Math.sin(omg12),
          domg12 = 1 + Math.cos(omg12),
          dbet1 = 1 + cbet1,
          dbet2 = 1 + cbet2;
        alp12 = 2 * Math.atan2(somg12 * (sbet1 * dbet2 + sbet2 * dbet1),
          domg12 * (sbet1 * sbet2 + dbet1 * dbet2));
      } else {
        var
          salp12 = salp2 * calp1 - calp2 * salp1,
          calp12 = calp2 * calp1 + salp2 * salp1;
        if (salp12 === 0 && calp12 < 0) {
          salp12 = g.tiny_ * calp1;
          calp12 = -1;
        }
        alp12 = Math.atan2(salp12, calp12);
      }
      vals.S12 += this._c2 * alp12;
      vals.S12 *= swapp * lonsign * latsign;
      vals.S12 += 0;
    }
    if (swapp < 0) {
      t = salp1;
      salp1 = salp2;
      salp2 = t;
      t = calp1;
      calp1 = calp2;
      calp2 = t;
      if (outmask & g.GEODESICSCALE) {
        t = vals.M12;
        vals.M12 = vals.M21;
        vals.M21 = t;
      }
    }
    salp1 *= swapp * lonsign;
    calp1 *= swapp * latsign;
    salp2 *= swapp * lonsign;
    calp2 *= swapp * latsign;
    if (outmask & g.AZIMUTH) {
      vals.azi1 = 0 - Math.atan2(-salp1, calp1) / m.degree;
      vals.azi2 = 0 - Math.atan2(-salp2, calp2) / m.degree;
    }
    return vals;
  };
  g.Geodesic.prototype.GenDirect = function(lat1, lon1, azi1,
    arcmode, s12_a12, outmask) {
    var line = new l.GeodesicLine(this, lat1, lon1, azi1,
      outmask | (arcmode ? g.NONE : g.DISTANCE_IN));
    return line.GenPosition(arcmode, s12_a12, outmask);
  };
  g.WGS84 = new g.Geodesic(GeographicLib.Constants.WGS84.a,
    GeographicLib.Constants.WGS84.f);
})();
// GeodesicLine.js
(function() {
  var g = GeographicLib.Geodesic;
  var l = GeographicLib.GeodesicLine;
  var m = GeographicLib.Math;
  l.GeodesicLine = function(geod, lat1, lon1, azi1, caps) {
    this._a = geod._a;
    this._f = geod._f;
    this._b = geod._b;
    this._c2 = geod._c2;
    this._f1 = geod._f1;
    this._caps = !caps ? g.ALL : (caps | g.LATITUDE | g.AZIMUTH);
    azi1 = g.AngRound(m.AngNormalize(azi1));
    this._lat1 = lat1;
    this._lon1 = lon1;
    this._azi1 = azi1;
    var alp1 = azi1 * m.degree;
    this._salp1 = azi1 === -180 ? 0 : Math.sin(alp1);
    this._calp1 = Math.abs(azi1) === 90 ? 0 : Math.cos(alp1);
    var cbet1, sbet1, phi;
    phi = lat1 * m.degree;
    sbet1 = this._f1 * Math.sin(phi);
    cbet1 = Math.abs(lat1) === 90 ? g.tiny_ : Math.cos(phi);
    var t = m.hypot(sbet1, cbet1);
    sbet1 /= t;
    cbet1 /= t;
    this._dn1 = Math.sqrt(1 + geod._ep2 * m.sq(sbet1));
    this._salp0 = this._salp1 * cbet1;
    this._calp0 = m.hypot(this._calp1, this._salp1 * sbet1);
    this._ssig1 = sbet1;
    this._somg1 = this._salp0 * sbet1;
    this._csig1 = this._comg1 =
      sbet1 !== 0 || this._calp1 !== 0 ? cbet1 * this._calp1 : 1;
    t = m.hypot(this._ssig1, this._csig1);
    this._ssig1 /= t;
    this._csig1 /= t;
    this._k2 = m.sq(this._calp0) * geod._ep2;
    var eps = this._k2 / (2 * (1 + Math.sqrt(1 + this._k2)) + this._k2);
    if (this._caps & g.CAP_C1) {
      this._A1m1 = g.A1m1f(eps);
      this._C1a = new Array(g.nC1_ + 1);
      g.C1f(eps, this._C1a);
      this._B11 = g.SinCosSeries(true, this._ssig1, this._csig1,
        this._C1a, g.nC1_);
      var s = Math.sin(this._B11),
        c = Math.cos(this._B11);
      this._stau1 = this._ssig1 * c + this._csig1 * s;
      this._ctau1 = this._csig1 * c - this._ssig1 * s;
    }
    if (this._caps & g.CAP_C1p) {
      this._C1pa = new Array(g.nC1p_ + 1);
      g.C1pf(eps, this._C1pa);
    }
    if (this._caps & g.CAP_C2) {
      this._A2m1 = g.A2m1f(eps);
      this._C2a = new Array(g.nC2_ + 1);
      g.C2f(eps, this._C2a);
      this._B21 = g.SinCosSeries(true, this._ssig1, this._csig1,
        this._C2a, g.nC2_);
    }
    if (this._caps & g.CAP_C3) {
      this._C3a = new Array(g.nC3_);
      geod.C3f(eps, this._C3a);
      this._A3c = -this._f * this._salp0 * geod.A3f(eps);
      this._B31 = g.SinCosSeries(true, this._ssig1, this._csig1,
        this._C3a, g.nC3_ - 1);
    }
    if (this._caps & g.CAP_C4) {
      this._C4a = new Array(g.nC4_);
      geod.C4f(eps, this._C4a);
      this._A4 = m.sq(this._a) * this._calp0 * this._salp0 * geod._e2;
      this._B41 = g.SinCosSeries(false, this._ssig1, this._csig1,
        this._C4a, g.nC4_);
    }
  };
  l.GeodesicLine.prototype.GenPosition = function(arcmode, s12_a12,
    outmask) {
    var vals = {};
    outmask &= this._caps & g.OUT_MASK;
    if (!(arcmode || (this._caps & g.DISTANCE_IN & g.OUT_MASK))) {
      vals.a12 = Number.NaN;
      return vals;
    }
    var sig12, ssig12, csig12, B12 = 0,
      AB1 = 0,
      ssig2, csig2;
    if (arcmode) {
      sig12 = s12_a12 * m.degree;
      var s12a = Math.abs(s12_a12);
      s12a -= 180 * Math.floor(s12a / 180);
      ssig12 = s12a === 0 ? 0 : Math.sin(sig12);
      csig12 = s12a === 90 ? 0 : Math.cos(sig12);
    } else {
      var
        tau12 = s12_a12 / (this._b * (1 + this._A1m1)),
        s = Math.sin(tau12),
        c = Math.cos(tau12);
      B12 = -g.SinCosSeries(true,
        this._stau1 * c + this._ctau1 * s,
        this._ctau1 * c - this._stau1 * s,
        this._C1pa, g.nC1p_);
      sig12 = tau12 - (B12 - this._B11);
      ssig12 = Math.sin(sig12);
      csig12 = Math.cos(sig12);
      if (Math.abs(this._f) > 0.01) {
        ssig2 = this._ssig1 * csig12 + this._csig1 * ssig12;
        csig2 = this._csig1 * csig12 - this._ssig1 * ssig12;
        B12 = g.SinCosSeries(true, ssig2, csig2, this._C1a, g.nC1_);
        var serr = (1 + this._A1m1) * (sig12 + (B12 - this._B11)) -
          s12_a12 / this._b;
        sig12 = sig12 - serr / Math.sqrt(1 + this._k2 * m.sq(ssig2));
        ssig12 = Math.sin(sig12);
        csig12 = Math.cos(sig12);
      }
    }
    var omg12, lam12, lon12;
    var sbet2, cbet2, somg2, comg2, salp2, calp2;
    ssig2 = this._ssig1 * csig12 + this._csig1 * ssig12;
    csig2 = this._csig1 * csig12 - this._ssig1 * ssig12;
    var dn2 = Math.sqrt(1 + this._k2 * m.sq(ssig2));
    if (outmask & (g.DISTANCE | g.REDUCEDLENGTH | g.GEODESICSCALE)) {
      if (arcmode || Math.abs(this._f) > 0.01)
        B12 = g.SinCosSeries(true, ssig2, csig2, this._C1a, g.nC1_);
      AB1 = (1 + this._A1m1) * (B12 - this._B11);
    }
    sbet2 = this._calp0 * ssig2;
    cbet2 = m.hypot(this._salp0, this._calp0 * csig2);
    if (cbet2 === 0)
      cbet2 = csig2 = g.tiny_;
    salp2 = this._salp0;
    calp2 = this._calp0 * csig2;
    if (outmask & g.DISTANCE)
      vals.s12 = arcmode ? this._b * ((1 + this._A1m1) * sig12 + AB1) : s12_a12;
    if (outmask & g.LONGITUDE) {
      somg2 = this._salp0 * ssig2;
      comg2 = csig2;
      omg12 = outmask & g.LONG_NOWRAP ? sig12 -
        (Math.atan2(ssig2, csig2) - Math.atan2(this._ssig1, this._csig1)) +
        (Math.atan2(somg2, comg2) - Math.atan2(this._somg1, this._comg1)) :
        Math.atan2(somg2 * this._comg1 - comg2 * this._somg1,
          comg2 * this._comg1 + somg2 * this._somg1);
      lam12 = omg12 + this._A3c *
        (sig12 + (g.SinCosSeries(true, ssig2, csig2, this._C3a, g.nC3_ - 1) -
          this._B31));
      lon12 = lam12 / m.degree;
      lon12 = m.AngNormalize2(lon12);
      vals.lon2 = outmask & g.LONG_NOWRAP ? this._lon1 + lon12 :
        m.AngNormalize(m.AngNormalize(this._lon1) + m.AngNormalize2(lon12));
    }
    if (outmask & g.LATITUDE)
      vals.lat2 = Math.atan2(sbet2, this._f1 * cbet2) / m.degree;
    if (outmask & g.AZIMUTH)
      vals.azi2 = 0 - Math.atan2(-salp2, calp2) / m.degree;
    if (outmask & (g.REDUCEDLENGTH | g.GEODESICSCALE)) {
      var
        B22 = g.SinCosSeries(true, ssig2, csig2, this._C2a, g.nC2_),
        AB2 = (1 + this._A2m1) * (B22 - this._B21),
        J12 = (this._A1m1 - this._A2m1) * sig12 + (AB1 - AB2);
      if (outmask & g.REDUCEDLENGTH)
        vals.m12 = this._b * ((dn2 * (this._csig1 * ssig2) -
            this._dn1 * (this._ssig1 * csig2)) -
          this._csig1 * csig2 * J12);
      if (outmask & g.GEODESICSCALE) {
        var t = this._k2 * (ssig2 - this._ssig1) * (ssig2 + this._ssig1) /
          (this._dn1 + dn2);
        vals.M12 = csig12 + (t * ssig2 - csig2 * J12) * this._ssig1 / this._dn1;
        vals.M21 = csig12 - (t * this._ssig1 - this._csig1 * J12) * ssig2 / dn2;
      }
    }
    if (outmask & g.AREA) {
      var
        B42 = g.SinCosSeries(false, ssig2, csig2, this._C4a, g.nC4_);
      var salp12, calp12;
      if (this._calp0 === 0 || this._salp0 === 0) {
        salp12 = salp2 * this._calp1 - calp2 * this._salp1;
        calp12 = calp2 * this._calp1 + salp2 * this._salp1;
        if (salp12 === 0 && calp12 < 0) {
          salp12 = g.tiny_ * this._calp1;
          calp12 = -1;
        }
      } else {
        salp12 = this._calp0 * this._salp0 *
          (csig12 <= 0 ? this._csig1 * (1 - csig12) + ssig12 * this._ssig1 :
            ssig12 * (this._csig1 * ssig12 / (1 + csig12) + this._ssig1));
        calp12 = m.sq(this._salp0) + m.sq(this._calp0) * this._csig1 * csig2;
      }
      vals.S12 = this._c2 * Math.atan2(salp12, calp12) +
        this._A4 * (B42 - this._B41);
    }
    vals.a12 = arcmode ? s12_a12 : sig12 / m.degree;
    return vals;
  };
})();
// PolygonArea.js
GeographicLib.PolygonArea = {};
(function() {
  var m = GeographicLib.Math;
  var a = GeographicLib.Accumulator;
  var g = GeographicLib.Geodesic;
  var p = GeographicLib.PolygonArea;
  p.transit = function(lon1, lon2) {
    lon1 = m.AngNormalize(lon1);
    lon2 = m.AngNormalize(lon2);
    var lon12 = m.AngDiff(lon1, lon2);
    var cross =
      lon1 < 0 && lon2 >= 0 && lon12 > 0 ? 1 :
      (lon2 < 0 && lon1 >= 0 && lon12 < 0 ? -1 : 0);
    return cross;
  };
  p.transitdirect = function(lon1, lon2) {
    lon1 = lon1 % 720.0;
    lon2 = lon2 % 720.0;
    return (((lon2 >= 0 && lon2 < 360) || lon2 < -360 ? 0 : 1) -
      ((lon1 >= 0 && lon1 < 360) || lon1 < -360 ? 0 : 1));
  };
  p.PolygonArea = function(earth, polyline) {
    this._earth = earth;
    this._area0 = 4 * Math.PI * earth._c2;
    this._polyline = !polyline ? false : polyline;
    this._mask = g.LATITUDE | g.LONGITUDE | g.DISTANCE |
      (this._polyline ? g.NONE : g.AREA | g.LONG_NOWRAP);
    if (!this._polyline)
      this._areasum = new a.Accumulator(0);
    this._perimetersum = new a.Accumulator(0);
    this.Clear();
  };
  p.PolygonArea.prototype.Clear = function() {
    this._num = 0;
    this._crossings = 0;
    if (!this._polyline)
      this._areasum.Set(0);
    this._perimetersum.Set(0);
    this._lat0 = this._lon0 = this._lat1 = this._lon1 = Number.NaN;
  };
  p.PolygonArea.prototype.AddPoint = function(lat, lon) {
    if (this._num === 0) {
      this._lat0 = this._lat1 = lat;
      this._lon0 = this._lon1 = lon;
    } else {
      var t = this._earth.Inverse(this._lat1, this._lon1, lat, lon, this._mask);
      this._perimetersum.Add(t.s12);
      if (!this._polyline) {
        this._areasum.Add(t.S12);
        this._crossings += p.transit(this._lon1, lon);
      }
      this._lat1 = lat;
      this._lon1 = lon;
    }
    ++this._num;
  };
  p.PolygonArea.prototype.AddEdge = function(azi, s) {
    if (this._num) {
      var t = this._earth.Direct(this._lat1, this._lon1, azi, s, this._mask);
      this._perimetersum.Add(s);
      if (!this._polyline) {
        this._areasum.Add(t.S12);
        this._crossings += p.transitdirect(this._lon1, t.lon2);
      }
      this._lat1 = t.lat2;
      this._lon1 = t.lon2;
    }
    ++this._num;
  };
  p.PolygonArea.prototype.Compute = function(reverse, sign) {
    var vals = {
      number: this._num
    };
    if (this._num < 2) {
      vals.perimeter = 0;
      if (!this._polyline)
        vals.area = 0;
      return vals;
    }
    if (this._polyline) {
      vals.perimeter = this._perimetersum.Sum();
      return vals;
    }
    var t = this._earth.Inverse(this._lat1, this._lon1, this._lat0, this._lon0,
      this._mask);
    vals.perimeter = this._perimetersum.Sum(t.s12);
    var tempsum = new a.Accumulator(this._areasum);
    tempsum.Add(t.S12);
    var crossings = this._crossings + p.transit(this._lon1, this._lon0);
    if (crossings & 1)
      tempsum.Add((tempsum.Sum() < 0 ? 1 : -1) * this._area0 / 2);
    if (!reverse)
      tempsum.Negate();
    if (sign) {
      if (tempsum.Sum() > this._area0 / 2)
        tempsum.Add(-this._area0);
      else if (tempsum.Sum() <= -this._area0 / 2)
        tempsum.Add(+this._area0);
    } else {
      if (tempsum.Sum() >= this._area0)
        tempsum.Add(-this._area0);
      else if (tempsum < 0)
        tempsum.Add(-this._area0);
    }
    vals.area = tempsum.Sum();
    return vals;
  };
  p.PolygonArea.prototype.TestPoint = function(lat, lon, reverse, sign) {
    var vals = {
      number: this._num + 1
    };
    if (this._num === 0) {
      vals.perimeter = 0;
      if (!this._polyline)
        vals.area = 0;
      return vals;
    }
    vals.perimeter = this._perimetersum.Sum();
    var tempsum = this._polyline ? 0 : this._areasum.Sum();
    var crossings = this._crossings;
    var t;
    for (var i = 0; i < (this._polyline ? 1 : 2); ++i) {
      t = this._earth.Inverse(
        i === 0 ? this._lat1 : lat, i === 0 ? this._lon1 : lon,
        i !== 0 ? this._lat0 : lat, i !== 0 ? this._lon0 : lon,
        this._mask);
      vals.perimeter += t.s12;
      if (!this._polyline) {
        tempsum += t.S12;
        crossings += p.transit(i === 0 ? this._lon1 : lon,
          i !== 0 ? this._lon0 : lon);
      }
    }
    if (this._polyline)
      return vals;
    if (crossings & 1)
      tempsum += (tempsum < 0 ? 1 : -1) * this._area0 / 2;
    if (!reverse)
      tempsum *= -1;
    if (sign) {
      if (tempsum > this._area0 / 2)
        tempsum -= this._area0;
      else if (tempsum <= -this._area0 / 2)
        tempsum += this._area0;
    } else {
      if (tempsum >= this._area0)
        tempsum -= this._area0;
      else if (tempsum < 0)
        tempsum += this._area0;
    }
    vals.area = tempsum;
    return vals;
  };
  p.PolygonArea.prototype.TestEdge = function(azi, s, reverse, sign) {
    var vals = {
      number: this._num ? this._num + 1 : 0
    };
    if (this._num === 0)
      return vals;
    vals.perimeter = this._perimetersum.Sum() + s;
    if (this._polyline)
      return vals;
    var tempsum = this._areasum.Sum();
    var crossings = this._crossings;
    var t;
    t = this._earth.Direct(this._lat1, this._lon1, azi, s, this._mask);
    tempsum += t.S12;
    crossings += p.transitdirect(this._lon1, t.lon2);
    t = this._earth(t.lat2, t.lon2, this._lat0, this._lon0, this._mask);
    perimeter += t.s12;
    tempsum += t.S12;
    crossings += p.transit(t.lon2, this._lon0);
    if (crossings & 1)
      tempsum += (tempsum < 0 ? 1 : -1) * this._area0 / 2;
    if (!reverse)
      tempsum *= -1;
    if (sign) {
      if (tempsum > this._area0 / 2)
        tempsum -= this._area0;
      else if (tempsum <= -this._area0 / 2)
        tempsum += this._area0;
    } else {
      if (tempsum >= this._area0)
        tempsum -= this._area0;
      else if (tempsum < 0)
        tempsum += this._area0;
    }
    vals.area = tempsum;
    return vals;
  };
  p.PolygonArea.prototype.CurrentPoint = function() {
    var vals = {
      lat: this._lat1,
      lon: this._lon1
    };
    return vals;
  };
  p.Area = function(earth, points, polyline) {
    var poly = new p.PolygonArea(earth, polyline);
    for (var i = 0; i < points.length; ++i)
      poly.AddPoint(points[i].lat, points[i].lon);
    return poly.Compute(false, true);
  };
})();
// DMS.js
GeographicLib.DMS = {};
(function() {
  var d = GeographicLib.DMS;
  var m = GeographicLib.Math;
  d.lookup = function(s, c) {
    return s.indexOf(c.toUpperCase());
  };
  d.zerofill = function(s, n) {
    return String("0000").substr(0, Math.max(0, Math.min(4, n - s.length))) +
      s;
  };
  d.hemispheres_ = "SNWE";
  d.signs_ = "-+";
  d.digits_ = "0123456789";
  d.dmsindicators_ = "D'\":";
  d.dmsindicatorsu_ = "\u00b0'\"";
  d.components_ = ["degrees", "minutes", "seconds"];
  d.NONE = 0;
  d.LATITUDE = 1;
  d.LONGITUDE = 2;
  d.AZIMUTH = 3;
  d.NUMBER = 4;
  d.DEGREE = 0;
  d.MINUTE = 1;
  d.SECOND = 2;
  d.Decode = function(dms) {
    var vals = {};
    var errormsg = new String("");
    var dmsa = dms;
    dmsa = dmsa.replace(/\u00b0/g, 'd');
    dmsa = dmsa.replace(/\u00ba/g, 'd');
    dmsa = dmsa.replace(/\u2070/g, 'd');
    dmsa = dmsa.replace(/\u02da/g, 'd');
    dmsa = dmsa.replace(/\u2032/g, '\'');
    dmsa = dmsa.replace(/\u00b4/g, '\'');
    dmsa = dmsa.replace(/\u2019/g, '\'');
    dmsa = dmsa.replace(/\u2033/g, '"');
    dmsa = dmsa.replace(/\u201d/g, '"');
    dmsa = dmsa.replace(/\u2212/g, '-');
    dmsa = dmsa.replace(/''/g, '"');
    dmsa = dmsa.replace(/^\s+/, "");
    dmsa = dmsa.replace(/\s+$/, "");
    do {
      var sign = 1;
      var beg = 0,
        end = dmsa.length;
      var ind1 = d.NONE;
      var k = -1;
      if (end > beg && (k = d.lookup(d.hemispheres_, dmsa.charAt(beg))) >= 0) {
        ind1 = (k & 2) ? d.LONGITUDE : d.LATITUDE;
        sign = (k & 1) ? 1 : -1;
        ++beg;
      }
      if (end > beg &&
        (k = d.lookup(d.hemispheres_, dmsa.charAt(end - 1))) >= 0) {
        if (k >= 0) {
          if (ind1 !== d.NONE) {
            if (dmsa.charAt(beg - 1).toUpperCase() ===
              dmsa.charAt(end - 1).toUpperCase())
              errormsg = "Repeated hemisphere indicators " +
              dmsa.charAt(beg - 1) + " in " +
              dmsa.substr(beg - 1, end - beg + 1);
            else
              errormsg = "Contradictory hemisphere indicators " +
              dmsa.charAt(beg - 1) + " and " + dmsa.charAt(end - 1) + " in " +
              dmsa.substr(beg - 1, end - beg + 1);
            break;
          }
          ind1 = (k & 2) ? d.LONGITUDE : d.LATITUDE;
          sign = (k & 1) ? 1 : -1;
          --end;
        }
      }
      if (end > beg && (k = d.lookup(d.signs_, dmsa.charAt(beg))) >= 0) {
        if (k >= 0) {
          sign *= k ? 1 : -1;
          ++beg;
        }
      }
      if (end === beg) {
        errormsg = "Empty or incomplete DMS string " + dmsa;
        break;
      }
      var ipieces = [0, 0, 0];
      var fpieces = [0, 0, 0];
      var npiece = 0;
      var icurrent = 0;
      var fcurrent = 0;
      var ncurrent = 0,
        p = beg;
      var pointseen = false;
      var digcount = 0;
      var intcount = 0;
      while (p < end) {
        var x = dmsa.charAt(p++);
        if ((k = d.lookup(d.digits_, x)) >= 0) {
          ++ncurrent;
          if (digcount > 0) {
            ++digcount;
          } else {
            icurrent = 10 * icurrent + k;
            ++intcount;
          }
        } else if (x === '.') {
          if (pointseen) {
            errormsg = "Multiple decimal points in " +
              dmsa.substr(beg, end - beg);
            break;
          }
          pointseen = true;
          digcount = 1;
        } else if ((k = d.lookup(d.dmsindicators_, x)) >= 0) {
          if (k >= 3) {
            if (p === end) {
              errormsg = "Illegal for : to appear at the end of " +
                dmsa.substr(beg, end - beg);
              break;
            }
            k = npiece;
          }
          if (k === npiece - 1) {
            errormsg = "Repeated " + d.components_[k] +
              " component in " + dmsa.substr(beg, end - beg);
            break;
          } else if (k < npiece) {
            errormsg = d.components_[k] + " component follows " +
              d.components_[npiece - 1] + " component in " +
              dmsa.substr(beg, end - beg);
            break;
          }
          if (ncurrent === 0) {
            errormsg = "Missing numbers in " + d.components_[k] +
              " component of " + dmsa.substr(beg, end - beg);
            break;
          }
          if (digcount > 1) {
            fcurrent = parseFloat(dmsa.substr(p - intcount - digcount - 1,
              intcount + digcount));
            icurrent = 0;
          }
          ipieces[k] = icurrent;
          fpieces[k] = icurrent + fcurrent;
          if (p < end) {
            npiece = k + 1;
            icurrent = fcurrent = 0;
            ncurrent = digcount = intcount = 0;
          }
        } else if (d.lookup(d.signs_, x) >= 0) {
          errormsg = "Internal sign in DMS string " +
            dmsa.substr(beg, end - beg);
          break;
        } else {
          errormsg = "Illegal character " + x + " in DMS string " +
            dmsa.substr(beg, end - beg);
          break;
        }
      }
      if (errormsg.length)
        break;
      if (d.lookup(d.dmsindicators_, dmsa.charAt(p - 1)) < 0) {
        if (npiece >= 3) {
          errormsg = "Extra text following seconds in DMS string " +
            dmsa.substr(beg, end - beg);
          break;
        }
        if (ncurrent === 0) {
          errormsg = "Missing numbers in trailing component of " +
            dmsa.substr(beg, end - beg);
          break;
        }
        if (digcount > 1) {
          fcurrent = parseFloat(dmsa.substr(p - intcount - digcount,
            intcount + digcount));
          icurrent = 0;
        }
        ipieces[npiece] = icurrent;
        fpieces[npiece] = icurrent + fcurrent;
      }
      if (pointseen && digcount === 0) {
        errormsg = "Decimal point in non-terminal component of " +
          dmsa.substr(beg, end - beg);
        break;
      }
      if (ipieces[1] >= 60) {
        errormsg = "Minutes " + fpieces[1] + " not in range [0, 60)";
        break;
      }
      if (ipieces[2] >= 60) {
        errormsg = "Seconds " + fpieces[2] + " not in range [0, 60)";
        break;
      }
      vals.ind = ind1;
      vals.val = sign * (fpieces[0] + (fpieces[1] + fpieces[2] / 60) / 60);
      return vals;
    } while (false);
    vals.val = d.NumMatch(dmsa);
    if (vals.val === 0)
      throw new Error(errormsg);
    else
      vals.ind = d.NONE;
    return vals;
  };
  d.NumMatch = function(s) {
    if (s.length < 3)
      return 0;
    var t = s.toUpperCase().replace(/0+$/, "");
    var sign = t.charAt(0) === '-' ? -1 : 1;
    var p0 = t.charAt(0) === '-' || t.charAt(0) === '+' ? 1 : 0;
    var p1 = t.length - 1;
    if (p1 + 1 < p0 + 3)
      return 0;
    t = t.substr(p0, p1 + 1 - p0);
    if (t === "NAN" || t === "1.#QNAN" || t === "1.#SNAN" || t === "1.#IND" ||
      t === "1.#R")
      return sign * Number.NaN;
    else if (t === "INF" || t === "1.#INF")
      return sign * Number.POSITIVE_INFINITY;
    return 0;
  };
  d.DecodeLatLon = function(stra, strb, swaplatlong) {
    var vals = {};
    if (!swaplatlong) swaplatlong = false;
    var valsa = d.Decode(stra);
    var valsb = d.Decode(strb);
    var a = valsa.val,
      ia = valsa.ind;
    var b = valsb.val,
      ib = valsb.ind;
    if (ia === d.NONE && ib === d.NONE) {
      ia = swaplatlong ? d.LONGITUDE : d.LATITUDE;
      ib = swaplatlong ? d.LATITUDE : d.LONGITUDE;
    } else if (ia === d.NONE)
      ia = d.LATITUDE + d.LONGITUDE - ib;
    else if (ib === d.NONE)
      ib = d.LATITUDE + d.LONGITUDE - ia;
    if (ia === ib)
      throw new Error("Both " + stra + " and " +
        strb + " interpreted as " +
        (ia === d.LATITUDE ? "latitudes" : "longitudes"));
    var lat = ia === d.LATITUDE ? a : b,
      lon = ia === d.LATITUDE ? b : a;
    if (Math.abs(lat) > 90)
      throw new Error("Latitude " + lat + "d not in [-90d, 90d]");
    if (lon < -540 || lon >= 540)
      throw new Error("Latitude " + lon + "d not in [-540d, 540d)");
    lon = m.AngNormalize(lon);
    vals.lat = lat;
    vals.lon = lon;
    return vals;
  };
  d.DecodeAngle = function(angstr) {
    var vals = d.Decode(angstr);
    var ang = vals.val,
      ind = vals.ind;
    if (ind !== d.NONE)
      throw new Error("Arc angle " + angstr +
        " includes a hemisphere, N/E/W/S");
    return ang;
  };
  d.DecodeAzimuth = function(azistr) {
    var vals = d.Decode(azistr);
    var azi = vals.val,
      ind = vals.ind;
    if (ind === d.LATITUDE)
      throw new Error("Azimuth " + azistr +
        " has a latitude hemisphere, N/S");
    if (azi < -540 || azi >= 540)
      throw new Error("Azimuth " + azistr + " not in range [-540d, 540d)");
    azi = m.AngNormalize(azi);
    return azi;
  };
  d.Encode = function(angle, trailing, prec, ind) {
    if (!ind) ind = d.NONE;
    if (!isFinite(angle))
      return angle < 0 ? String("-inf") :
        (angle > 0 ? String("inf") : String("nan"));
    prec = Math.min(15 - 2 * trailing, prec);
    var scale = 1,
      i;
    for (i = 0; i < trailing; ++i)
      scale *= 60;
    for (i = 0; i < prec; ++i)
      scale *= 10;
    if (ind === d.AZIMUTH)
      angle -= Math.floor(angle / 360) * 360;
    var sign = angle < 0 ? -1 : 1;
    angle *= sign;
    var
      idegree = Math.floor(angle),
      fdegree = Math.floor((angle - idegree) * scale + 0.5) / scale;
    if (fdegree >= 1) {
      idegree += 1;
      fdegree -= 1;
    }
    var pieces = [fdegree, 0, 0];
    for (i = 1; i <= trailing; ++i) {
      var
        ip = Math.floor(pieces[i - 1]),
        fp = pieces[i - 1] - ip;
      pieces[i] = fp * 60;
      pieces[i - 1] = ip;
    }
    pieces[0] += idegree;
    var s = new String("");
    if (ind === d.NONE && sign < 0)
      s += '-';
    switch (trailing) {
      case d.DEGREE:
        s += d.zerofill(pieces[0].toFixed(prec),
            ind === d.NONE ? 0 :
            1 + Math.min(ind, 2) + prec + (prec ? 1 : 0)) +
          d.dmsindicatorsu_.charAt(0);
        break;
      default:
        s += d.zerofill(pieces[0].toFixed(0),
            ind === d.NONE ? 0 : 1 + Math.min(ind, 2)) +
          d.dmsindicatorsu_.charAt(0);
        switch (trailing) {
          case d.MINUTE:
            s += d.zerofill(pieces[1].toFixed(prec), 2 + prec + (prec ? 1 : 0)) +
              d.dmsindicatorsu_.charAt(1);
            break;
          case d.SECOND:
            s += d.zerofill(pieces[1].toFixed(0), 2) + d.dmsindicatorsu_.charAt(1);
            s += d.zerofill(pieces[2].toFixed(prec), 2 + prec + (prec ? 1 : 0)) +
              d.dmsindicatorsu_.charAt(2);
            break;
          default:
            break;
        }
    }
    if (ind !== d.NONE && ind !== d.AZIMUTH)
      s += d.hemispheres_.charAt((ind === d.LATITUDE ? 0 : 2) +
        (sign < 0 ? 0 : 1));
    return s;
  };
})();
// Interface.js
(function() {
  var m = GeographicLib.Math;
  var g = GeographicLib.Geodesic;
  var l = GeographicLib.GeodesicLine;
  g.Geodesic.CheckPosition = function(lat, lon) {
    if (!(Math.abs(lat) <= 90))
      throw new Error("latitude " + lat + " not in [-90, 90]");
    if (!(lon >= -540 && lon < 540))
      throw new Error("longitude " + lon + " not in [-540, 540)");
    return m.AngNormalize(lon);
  };
  g.Geodesic.CheckAzimuth = function(azi) {
    if (!(azi >= -540 && azi < 540))
      throw new Error("longitude " + azi + " not in [-540, 540)");
    return m.AngNormalize(azi);
  };
  g.Geodesic.CheckDistance = function(s) {
    if (!(isFinite(s)))
      throw new Error("distance " + s + " not a finite number");
  };
  g.Geodesic.prototype.Inverse = function(lat1, lon1, lat2, lon2, outmask) {
    if (!outmask) outmask = g.DISTANCE | g.AZIMUTH;
    lon1 = g.Geodesic.CheckPosition(lat1, lon1);
    lon2 = g.Geodesic.CheckPosition(lat2, lon2);
    var result = this.GenInverse(lat1, lon1, lat2, lon2, outmask);
    result.lat1 = lat1;
    result.lon1 = lon1;
    result.lat2 = lat2;
    result.lon2 = lon2;
    return result;
  };
  g.Geodesic.prototype.Direct = function(lat1, lon1, azi1, s12, outmask) {
    if (!outmask) outmask = g.LATITUDE | g.LONGITUDE | g.AZIMUTH;
    lon1 = g.Geodesic.CheckPosition(lat1, lon1);
    azi1 = g.Geodesic.CheckAzimuth(azi1);
    g.Geodesic.CheckDistance(s12);
    var result = this.GenDirect(lat1, lon1, azi1, false, s12, outmask);
    result.lat1 = lat1;
    result.lon1 = lon1;
    result.azi1 = azi1;
    result.s12 = s12;
    return result;
  };
  g.Geodesic.prototype.InversePath =
    function(lat1, lon1, lat2, lon2, ds12, maxk) {
      var t = this.Inverse(lat1, lon1, lat2, lon2);
      if (!maxk) maxk = 20;
      if (!(ds12 > 0))
        throw new Error("ds12 must be a positive number");
      var
        k = Math.max(1, Math.min(maxk, Math.ceil(t.s12 / ds12))),
        points = new Array(k + 1);
      points[0] = {
        lat: t.lat1,
        lon: t.lon1,
        azi: t.azi1
      };
      points[k] = {
        lat: t.lat2,
        lon: t.lon2,
        azi: t.azi2
      };
      if (k > 1) {
        var line = new l.GeodesicLine(this, t.lat1, t.lon1, t.azi1,
            g.LATITUDE | g.LONGITUDE | g.AZIMUTH),
          da12 = t.a12 / k;
        var vals;
        for (var i = 1; i < k; ++i) {
          vals =
            line.GenPosition(true, i * da12, g.LATITUDE | g.LONGITUDE | g.AZIMUTH);
          points[i] = {
            lat: vals.lat2,
            lon: vals.lon2,
            azi: vals.azi2
          };
        }
      }
      return points;
    };
  g.Geodesic.prototype.DirectPath =
    function(lat1, lon1, azi1, s12, ds12, maxk) {
      var t = this.Direct(lat1, lon1, azi1, s12);
      if (!maxk) maxk = 20;
      if (!(ds12 > 0))
        throw new Error("ds12 must be a positive number");
      var
        k = Math.max(1, Math.min(maxk, Math.ceil(Math.abs(t.s12) / ds12))),
        points = new Array(k + 1);
      points[0] = {
        lat: t.lat1,
        lon: t.lon1,
        azi: t.azi1
      };
      points[k] = {
        lat: t.lat2,
        lon: t.lon2,
        azi: t.azi2
      };
      if (k > 1) {
        var line = new l.GeodesicLine(this, t.lat1, t.lon1, t.azi1,
            g.LATITUDE | g.LONGITUDE | g.AZIMUTH),
          da12 = t.a12 / k;
        var vals;
        for (var i = 1; i < k; ++i) {
          vals =
            line.GenPosition(true, i * da12, g.LATITUDE | g.LONGITUDE | g.AZIMUTH);
          points[i] = {
            lat: vals.lat2,
            lon: vals.lon2,
            azi: vals.azi2
          };
        }
      }
      return points;
    };
  g.Geodesic.prototype.Circle = function(lat1, lon1, azi1, s12, k) {
    if (!(Math.abs(lat1) <= 90))
      throw new Error("lat1 must be in [-90, 90]");
    if (!(lon1 >= -540 && lon1 < 540))
      throw new Error("lon1 must be in [-540, 540)");
    if (!(azi1 >= -540 && azi1 < 540))
      throw new Error("azi1 must be in [-540, 540)");
    if (!(isFinite(s12)))
      throw new Error("s12 must be a finite number");
    lon1 = m.AngNormalize(lon1);
    azi1 = m.AngNormalize(azi1);
    if (!k || k < 4) k = 24;
    var points = new Array(k + 1);
    var vals;
    for (var i = 0; i <= k; ++i) {
      var azi1a = azi1 + (k - i) * 360 / k;
      if (azi1a >= 180) azi1a -= 360;
      vals =
        this.GenDirect(lat1, lon1, azi1a, false, s12, g.LATITUDE | g.LONGITUDE);
      points[i] = {
        lat: vals.lat2,
        lon: vals.lon2
      };
    }
    return points;
  };
  g.Geodesic.prototype.Envelope = function(lat1, lon1, k, ord) {
    if (!(Math.abs(lat1) <= 90))
      throw new Error("lat1 must be in [-90, 90]");
    if (!(lon1 >= -540 && lon1 < 540))
      throw new Error("lon1 must be in [-540, 540)");
    lon1 = m.AngNormalize(lon1);
    if (!k || k < 4) k = 24;
    if (!ord) ord = 1;
    var points = new Array(k + 1);
    var vals, line, s12, j;
    for (var i = 0; i <= k; ++i) {
      var azi1 = -180 + i * 360 / k;
      line = new l.GeodesicLine(this, lat1, lon1, azi1,
        g.LATITUDE | g.LONGITUDE | g.DISTANCE_IN |
        g.DISTANCE | g.REDUCEDLENGTH | g.GEODESICSCALE);
      vals = line.GenPosition(true, 180 * ord,
        g.DISTANCE | g.REDUCEDLENGTH | g.GEODESICSCALE);
      j = 0;
      while (true) {
        s12 = vals.s12 - vals.m12 / vals.M21;
        if (Math.abs(vals.m12) < line._a * g.tol2_ * 0.1 || ++j > 10)
          break;
        vals = line.GenPosition(false, s12,
          g.DISTANCE | g.REDUCEDLENGTH | g.GEODESICSCALE);
      }
      vals = line.GenPosition(false, s12, g.LATITUDE | g.LONGITUDE);
      points[i] = {
        lat: vals.lat2,
        lon: vals.lon2
      };
    }
    return points;
  };
  g.Geodesic.prototype.Area = function(points, polyline) {
    return GeographicLib.PolygonArea.Area(this, points, polyline);
  };
})();
module.exports = GeographicLib;
