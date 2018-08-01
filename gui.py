from tkinter import *
import os


global dna
dna = Tk()

semaphore1 = [ 'semaphore1', True]
semaphore2 = [ 'semaphore2', True]

command_to_spark = ['COMMAND', 'spark-submit --master local DNA.py ']

def Ssubmit(cmd):
    print(cmd)
    os.system('dir')
    os.system(cmd)
    
    

def submit_cross(var,cmp):
    command_to_spark[1] = command_to_spark[1] + ' ' + var.get()
    if(var.get() == '-CMA'):
        command_to_spark[1] = command_to_spark[1] + ' ' + cmp
        
    print(command_to_spark[1])
    cm = Tk()
    label = Label( cm, text = 'COMMAND IS: ' + command_to_spark[1], font = 50, relief = RAISED )
    label.pack()
    Ssubmit(command_to_spark[1])
    dna.destroy()


def submit_ohlc(var,month,cmp):
    if(var.get() == 1):
        command_to_spark[1] = command_to_spark[1] + ' ' + month + ' ' + cmp
        
    elif(var.get() == 2):
        command_to_spark[1] = command_to_spark[1] + ' ' + cmp 
        
    print(command_to_spark[1])
    cm = Tk()
    label = Label(cm, text = 'COMMAND IS: ' + command_to_spark[1], font = 50, relief = RAISED )
    label.pack()
    Ssubmit(command_to_spark[1])
    dna.destroy()
    
    
def cross_cmd(ss):
    if(semaphore2[1]):
        command_to_spark[1] = command_to_spark[1] + 'cross ' + ss
        if(ss == 'dates'):
            command_to_spark[1] = command_to_spark[1] + ' ' + str(listbox11.get(listbox11.curselection())) + '/' + str(listbox12.get(listbox12.curselection())) + '/' + str(listbox13.get(listbox13.curselection()))[2:4] + '_to_' + str(listbox21.get(listbox21.curselection())) + '/' + str(listbox22.get(listbox22.curselection())) + '/' + str(listbox23.get(listbox23.curselection()))[2:4]

        elif(ss == 'year'):
            command_to_spark[1] = command_to_spark[1] + ' ' + str(listbox.get(listbox.curselection()))

        var = StringVar()
        R1 = Radiobutton(dna, text = "-CSA", variable = var, value = "-CSA")
        R1.place(x = 300, y = 450)

        R2 = Radiobutton(dna, text = "-CSA --mCCA", variable = var, value = "-CSA --mCCA")
        R2.place(x = 300, y = 480)

        R3 = Radiobutton(dna, text = "-CSA --mCCA --mCMA", variable = var, value = "-CSA --mCCA --mCMA")
        R3.place(x = 300, y = 510)

        R4 = Radiobutton(dna, text = "-CSA --mCMA", variable = var, value = "-CSA --mCMA")
        R4.place(x = 300, y = 540)

        R5 = Radiobutton(dna, text = "-CMA", variable = var, value = "-CMA")
        R5.place(x = 300, y = 570)

        Lab = Label(dna, font = 20, text = 'company')
        Lab.place(x = 375, y = 570)
        En = Entry(dna, bd = 5, width = 6)
        En.place(x = 450,y = 570)


        Bt = Button(dna, height = 4, width = 20, text = 'SUBMIT' ,bg = 'red', command = lambda : submit_cross(var,En.get())) 
        Bt.place(x = 600,y = 500)
        print(command_to_spark[1])
        semaphore2[1] = False


def ohlc_cmd(ss):
    if(semaphore2[1]):
        command_to_spark[1] = command_to_spark[1] + 'ohlc ' + ss
        if(ss == 'dates'):
            command_to_spark[1] = command_to_spark[1] + ' ' + str(listbox11.get(listbox11.curselection())) + '/' + str(listbox12.get(listbox12.curselection())) + '/' + str(listbox13.get(listbox13.curselection()))[2:4] + '_to_' + str(listbox21.get(listbox21.curselection())) + '/' + str(listbox22.get(listbox22.curselection())) + '/' + str(listbox23.get(listbox23.curselection()))[2:4]

        elif(ss == 'year'):
            command_to_spark[1] = command_to_spark[1] + ' ' + str(listbox.get(listbox.curselection()))

        var = IntVar()
        Lab1 = Label(dna, text = 'month FORMAT: Mon')
        Lab1.place(x = 300, y = 450)
        En1 = Entry(dna, bd = 5, width = 6)
        En1.place(x = 450,y = 450)

        Lab2 = Label(dna, text = 'company')
        Lab2.place(x = 300, y = 480)
        En2 = Entry(dna, bd = 5, width = 6)
        En2.place(x = 450,y = 480)

        
        R1 = Radiobutton(dna, text = 'monthly',  variable = var, value = 1)
        R1.place(x = 300, y = 550)

        R2 = Radiobutton(dna, text = 'total',  variable = var, value = 2)
        R2.place(x = 300, y = 570)



        Bt = Button(dna, height = 4, width = 20,text = 'SUBMIT', bg = 'red',command = lambda : submit_ohlc(var,En1.get(),En2.get())) 
        Bt.place(x = 600,y = 600)
            
        print(command_to_spark[1])
        semaphore2[1] = False



def show_cmd(ss):
    dna.ohlc = PhotoImage(file = r"C:\Users\HP\Pictures\Screenshots\ohlc.png")
    dna.cross = PhotoImage(file = r"C:\Users\HP\Pictures\Screenshots\cross.png")
    
    B1 = Button(dna, height = 75, width = 150, image = dna.cross,
                command = lambda : cross_cmd(ss))
    B2 = Button(dna, height = 75, width = 150, image = dna.ohlc ,
                command = lambda : ohlc_cmd(ss))

    B1.place(x = 10,y = 470)
    B2.place(x = 10,y = 570)

def select(ss):
    if(semaphore1[1]):
        semaphore1[1] = False
        #command_to_spark[1] = command_to_spark[1] + ss
        print(command_to_spark)
        show = Button(dna , text = 'SHOW \n COMMANDS', height = 3, width = 15,
                      bg = 'skyblue' ,font = 10,command = lambda : show_cmd(ss))
        show.place(x = 600, y = 300) 
        return
    

pic = PhotoImage(file = r"C:\Users\HP\Pictures\Screenshots\iimg.png")
C = Canvas(dna, bg = 'white',height = 1000, width = 1000)
image = C.create_image(700, 10, anchor = NE, image = pic)
C.pack()


L = Label(dna, text = "year FORMAT: YYYY", width = 20)
L.place(x = 10, y = 310)

############## adding list for date ################
scrollbar = Scrollbar(dna,orient="vertical")
scrollbar.pack(side=RIGHT, fill=Y)

listbox = Listbox(dna, height = 5,exportselection=0)
listbox.place(x = 10, y = 340)
for i in range(5):
    listbox.insert(END, i+2009)

listbox.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar.config(command=listbox.yview)
######################################################
      

L1 = Label(dna, text = "from date: DD/MM/YY", width = 18)
L2 = Label(dna, text = "till date: DD/MM/YY", width = 18)
L1.place(x = 220, y = 315)
L2.place(x = 400, y = 315)


############## adding list for date ################
scrollbar11 = Scrollbar(dna,orient="vertical")
scrollbar11.pack(side=RIGHT, fill=Y)

listbox11 = Listbox(dna, height = 5, width = 4,exportselection=0)
listbox11.place(x = 220, y = 340)
for i in range(31):
    listbox11.insert(END, i+1)

listbox11.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar11.config(command=listbox.yview)
######################################################

############## adding list for date ################
scrollbar12 = Scrollbar(dna, orient="vertical")
scrollbar12.pack(side=RIGHT, fill=Y)

listbox12 = Listbox(dna, height = 5 , width = 3,exportselection=0)
listbox12.place(x = 260, y = 340)

for i in range(12):
    listbox12.insert(END, i+1)

listbox12.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar12.config(command=listbox.yview)
######################################################

############## adding list for date ################
scrollbar13 = Scrollbar(dna, orient="vertical")
scrollbar13.pack(side=RIGHT, fill=Y)

listbox13 = Listbox(dna, height = 5 , width = 8,exportselection=0)
listbox13.place(x = 290, y = 340)

for i in range(5):
    listbox13.insert(END, i+2009)

listbox13.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar13.config(command=listbox.yview)
######################################################



############## adding list for date ################
scrollbar21 = Scrollbar(dna,orient="vertical")
scrollbar21.pack(side=RIGHT, fill=Y)

listbox21 = Listbox(dna, height = 5, width = 4,exportselection=0)
listbox21.place(x = 400, y = 340)
for i in range(31):
    listbox21.insert(END, i+1)

listbox21.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar21.config(command=listbox.yview)
######################################################

############## adding list for date ################
scrollbar22 = Scrollbar(dna, orient="vertical")
scrollbar22.pack(side=RIGHT, fill=Y)

listbox22 = Listbox(dna, height = 5 , width = 3,exportselection=0)
listbox22.place(x = 440, y = 340)

for i in range(12):
    listbox22.insert(END, i+1)

listbox22.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar22.config(command=listbox.yview)
######################################################

############## adding list for date ################
scrollbar23 = Scrollbar(dna, orient="vertical")
scrollbar23.pack(side=RIGHT, fill=Y)

listbox23 = Listbox(dna, height = 5 , width = 8,exportselection=0)
listbox23.place(x = 470, y = 340)

for i in range(5):
    listbox23.insert(END, i+2009)

listbox23.config(yscrollcommand=scrollbar.set, selectmode = SINGLE )
scrollbar23.config(command=listbox.yview)
######################################################









Byc = Button(dna , text = 'YEAR', height = 2, width = 10, bg = 'skyblue' ,
                     command = lambda : select('year'))
Byc.place(x = 30, y = 250)

Bdc = Button(dna , text = 'DATES', height = 2, width = 30, bg = 'skyblue' ,
                    command = lambda : select('dates'))

Bdc.place(x = 250, y = 250)

dna.mainloop()

