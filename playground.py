import pandas as pd
import time

from py_vollib.black_scholes.greeks import analytical as bs_a
from py_vollib.black_scholes.greeks import numerical as bs_n
from py_vollib.black_scholes.implied_volatility import implied_volatility as IV

starttime = time.time()
print("Loading File...")
sample = pd.read_csv("big_files/ready_options.csv")
loadingtime = time.time()
print("Loading took: ", (loadingtime - starttime)/60, " minutes.")


print(list(sample))
print(len(list(sample)))

def Calculate_IV(each_row):
    try:
        measure = IV(price=each_row['Close'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                  t=each_row['Days_to_Expiry']/365, r=.1, flag=each_row['CallOrPut'])
    except:
        measure = 0
    print ("IV", end= " ")
    return measure

def Calculate_delta(each_row):
    if each_row['Implied_Volatility'] == 0:
        measure= 0
    else:
        try:
            measure = bs_a.delta(flag=each_row['CallOrPut'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                 t=each_row['Days_to_Expiry']/365, r=.1,sigma=each_row['Implied_Volatility'])
        except:
            measure = 0

    print("Delta", end = " ")
    return measure


def Calculate_gamma(each_row):
    if each_row['Implied_Volatility'] == 0:
        measure= 0
    else:
        try:
            measure = bs_a.gamma(flag=each_row['CallOrPut'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                 t=each_row['Days_to_Expiry']/365, r=.1,sigma=each_row['Implied_Volatility'])
        except:
            measure = 0

    print("Gamma", end=" ")
    return measure

def Calculate_theta(each_row):
    if each_row['Implied_Volatility'] == 0:
        measure= 0
    else:
        try:
            measure = bs_a.theta(flag=each_row['CallOrPut'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                 t=each_row['Days_to_Expiry']/365, r=.1,sigma=each_row['Implied_Volatility'])
        except:
            measure = 0

    print("Theta", end=" ")
    return measure

def Calculate_vega(each_row):
    if each_row['Implied_Volatility'] == 0:
        measure= 0
    else:
        try:
            measure = bs_a.vega(flag=each_row['CallOrPut'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                 t=each_row['Days_to_Expiry']/365, r=.1,sigma=each_row['Implied_Volatility'])
        except:
            measure = 0
    print("Vega", end=" ")
    return measure


def Calculate_rho(each_row):
    if each_row['Implied_Volatility'] == 0:
        measure = 0
    else:
        try:
            measure = bs_a.rho(flag=each_row['CallOrPut'], S=each_row['Spot'], K=each_row['Strike_Price'],
                                 t=each_row['Days_to_Expiry']/365, r=.1,sigma=each_row['Implied_Volatility'])
        except:
            measure = 0
    print("Rho", end=" ")
    return measure




sample['Implied_Volatility'] = sample.apply(Calculate_IV, axis= 1)

IV_time = time.time()
print("\n IV took: ", (IV_time - loadingtime)/60)

sample['Delta'] = sample.apply(Calculate_delta, axis=1)

delta_time = time.time()
print("\n Delta took: ", (delta_time - IV_time)/60)


sample['Gamma'] = sample.apply(Calculate_gamma, axis=1)

gamma_time = time.time()
print("\n Gamma took: ", (gamma_time - delta_time)/60)

sample['Vega'] = sample.apply(Calculate_vega, axis=1)

Vega_time = time.time()
print("\n Vega took: ", (Vega_time - gamma_time)/60)


sample['Theta'] = sample.apply(Calculate_theta, axis=1)

theta_time = time.time()
print("\n theta took: ", (theta_time - Vega_time)/60)

sample['Rho'] = sample.apply(Calculate_rho, axis=1)
rho_time= time.time()
print("\n Rho took: ", (rho_time -theta_time)/60)


sample.to_csv("big_files/ready_options_output.csv", index=False)
