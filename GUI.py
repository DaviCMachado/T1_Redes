from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.image import Image
from kivy.uix.popup import Popup
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.scrollview import ScrollView
from kivy.uix.anchorlayout import AnchorLayout
from kivy.graphics import Color, Rectangle
from kivy.app import App
import os


class TelaMenuInicial(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        layout = FloatLayout()

        self.graficos = []  # Lista para armazenar os gráficos gerados
        self.indice_atual = 0  # Índice atual do gráfico exibido


        btn_gerar = Button(text="Gráficos de Rede", size_hint=(0.4, 0.2), pos_hint={"center_x": 0.5, "center_y": 0.75})
        btn_gerar.bind(on_press=self.ir_para_entrada)
        layout.add_widget(btn_gerar)

        btn_stats = Button(text="Análise Estatística", size_hint=(0.4, 0.2), pos_hint={"center_x": 0.5, "center_y": 0.5})
        btn_stats.bind(on_press=self.ir_para_analise)
        layout.add_widget(btn_stats)

        btn_sobre = Button(text="Sobre", size_hint=(0.4, 0.2), pos_hint={"center_x": 0.5, "center_y": 0.25})
        btn_sobre.bind(on_press=self.abrir_popup_sobre)
        layout.add_widget(btn_sobre)

        self.add_widget(layout)


    def ir_para_entrada(self, *args):
        self.manager.current = "Graficos Basicos"

    def ir_para_analise(self, *args):
        self.manager.current = "Analise"

    def abrir_popup_sobre(self, *args):
        texto1 = (
            "Trabalho 1 Rede de Computadores.\n\n"
            "Este programa tem como objetivo analisar e gerar gráficos a partir de arquivos de captura de pacotes de IP.\n\n"
        )

        texto2 = (
            "Feito com amor por:\n\nDavi de Castro Machado\nGiovana Borelli\nJoão Pedro Righi\nLaura Boemo"
        )

        # Layout principal com padding
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)

        # Botão X de fechar (alinhado à direita)
        fechar_layout = AnchorLayout(anchor_x='right', anchor_y='top', size_hint=(1, None), height=40)
        btn_fechar = Button(text='X', size_hint=(None, None), size=(40, 40))
        fechar_layout.add_widget(btn_fechar)
        content.add_widget(fechar_layout)

        # Scroll com o texto principal (texto1)
        scroll = ScrollView(size_hint=(1, 1))
        label = Label(
            text=texto1,
            size_hint_y=None,
            halign='left',
            valign='top',
            font_size='16sp',
            text_size=(660, None),
            padding=(20, 10)  # Padding nas laterais e topo/baixo
        )
        label.bind(texture_size=lambda instance, value: setattr(label, 'height', value[1]))
        scroll.add_widget(label)
        content.add_widget(scroll)

        # Adicionando o texto2 (centralizado horizontalmente)
        label2 = Label(
            text=texto2,
            size_hint_y=0.8,
            halign='center',  # Centraliza horizontalmente
            valign='middle',  # Centraliza verticalmente dentro do espaço
            font_size='16sp',
            text_size=(660, None),
            padding=(20, 10)
        )
        content.add_widget(label2)

        # Popup
        popup = Popup(title="Sobre o trabalho",
                    content=content,
                    size_hint=(0.9, 0.7),
                    auto_dismiss=False)

        # Botão fecha o popup
        btn_fechar.bind(on_release=popup.dismiss)
        popup.open()

class TelaAnalise(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Define os gráficos em listas separadas para cada tipo
        self.graficos_barras = [
            ("img/barras/protocolos.png", "Frequência de Protocolos"),
            ("img/barras/ip_origem.png", "Top IPs de Origem"),
            ("img/barras/ip_destino.png", "Top IPs de Destino")
        ]
        self.graficos_histograma = [
            ("img/histograms/tamanhos.png", "Distribuição dos Tamanhos dos Pacotes")
        ]
        self.graficos_pizza = [
            ("img/pizza/protocolos_pizza.png", "Proporção de Protocolos"),
            ("img/pizza/ip_origem_pizza.png", "Proporção dos Principais IPs de Origem"),
            ("img/pizza/ip_destino_pizza.png", "Proporção dos Principais IPs de Destino")
        ]
        self.graficos_heatmap = [
            ("img/heatmap/correlacao.png", "Mapa de Calor de Protocolos")

        ]
        self.graficos_tempo = [
            ("img/tempo/pacotes_tempo.png", "Pacotes por Tempo")
        ]

        # Inicializa a lista atual com uma lista vazia (será atribuída conforme o botão)
        self.graficos_atual = []
        self.indice_atual = 0  # Índice do gráfico atual

        layout_principal = BoxLayout(orientation='horizontal')

        # Barra lateral (25% da tela)
        barra_lateral = BoxLayout(orientation='vertical', size_hint=(0.25, 1), spacing=10, padding=10)

        label = Label(text="Tipos de Gráfico", size_hint=(1, 0.1))
        barra_lateral.add_widget(label)

        # Botões para alternar a exibição dos gráficos
        btn_barra = Button(text="Gráfico de Barra", size_hint=(1, 0.1))
        btn_barra.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_barra)

        btn_histograma = Button(text="Histograma", size_hint=(1, 0.1))
        btn_histograma.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_histograma)

        btn_pizza = Button(text="Gráfico de Pizza", size_hint=(1, 0.1))
        btn_pizza.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_pizza)

        btn_heatmap = Button(text="Mapa de Calor", size_hint=(1, 0.1))
        btn_heatmap.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_heatmap)

        btn_tempo = Button(text="Gráfico Temporal", size_hint=(1, 0.1))
        btn_tempo.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_tempo)

        # Botão de voltar
        btn_voltar = Button(text="Voltar", size_hint=(1, 0.1))
        btn_voltar.bind(on_press=self.voltar_menu)
        barra_lateral.add_widget(btn_voltar)

        layout_principal.add_widget(barra_lateral)

        # Área central (75% da tela) para o gráfico e explicação
        self.area_grafico = BoxLayout(orientation='vertical', size_hint=(0.75, 1), padding=20, spacing=10)

        # Fundo branco (ajustando para ocupar até 80% da altura)
        self.fundo_branco = FloatLayout(size_hint=(1.0, 0.8))
        with self.fundo_branco.canvas.before:
            Color(1, 1, 1, 1)  # cor do fundo branco
            self.fundo_rect = Rectangle(pos=self.fundo_branco.pos, size=self.fundo_branco.size)
            self.fundo_branco.bind(pos=self._update_fundo, size=self._update_fundo)

        # A imagem ocupa 80% do fundo branco e fica centralizada
        self.imagem = Image(
            size_hint=(0.8, 0.8),
            pos_hint={"center_x": 0.5, "center_y": 0.5},
            allow_stretch=True,
            keep_ratio=True
        )
        self.fundo_branco.add_widget(self.imagem)

        # Adicionar o fundo branco à área de gráficos
        self.area_grafico.add_widget(self.fundo_branco)

        # Label explicativo
        self.label_explicativo = Label(
            text="Legenda do gráfico aparecerá aqui",
            size_hint=(1, 0.1),
            halign='center'
        )
        self.area_grafico.add_widget(self.label_explicativo)

        # Botões de navegação
        box_nav = BoxLayout(size_hint=(1, 0.1), spacing=20, padding=(40, 0))
        btn_anterior = Button(text="Anterior")
        btn_proximo = Button(text="Próximo")
        btn_anterior.bind(on_press=self.anterior_grafico)
        btn_proximo.bind(on_press=self.proximo_grafico)
        box_nav.add_widget(btn_anterior)
        box_nav.add_widget(btn_proximo)
        self.area_grafico.add_widget(box_nav)

        layout_principal.add_widget(self.area_grafico)
        self.add_widget(layout_principal)

    def alternar_grafico(self, instance):
        # Dependendo do botão clicado, altera a lista de gráficos atual
        if instance.text == "Gráfico de Barra":
            self.graficos_atual = self.graficos_barras
        elif instance.text == "Histograma":
            self.graficos_atual = self.graficos_histograma
        elif instance.text == "Gráfico de Pizza":
            self.graficos_atual = self.graficos_pizza

        self.indice_atual = 0  # Reseta o índice ao selecionar um novo tipo de gráfico
        self.atualizar_imagem()

    def atualizar_imagem(self):
        if self.graficos_atual:  # Verifica se há gráficos na lista atual
            caminho_imagem, legenda = self.graficos_atual[self.indice_atual]
            if os.path.exists(caminho_imagem):
                self.imagem.source = caminho_imagem
                self.imagem.reload()
            self.label_explicativo.text = legenda

    def anterior_grafico(self, *args):
        if self.indice_atual > 0:
            self.indice_atual -= 1
            self.atualizar_imagem()

    def proximo_grafico(self, *args):
        if self.indice_atual < len(self.graficos_atual) - 1:
            self.indice_atual += 1
            self.atualizar_imagem()

    def voltar_menu(self, *args):
        self.manager.current = "Menu Inicial"

    def _update_fundo(self, instance, value):
        self.fundo_rect.pos = instance.pos
        self.fundo_rect.size = instance.size

class TelaGraficosBasicos(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Define os gráficos em listas separadas para cada tipo
        self.graficos_barras = [
            ("img/barras/protocolos.png", "Frequência de Protocolos"),
            ("img/barras/ip_origem.png", "Top IPs de Origem"),
            ("img/barras/ip_destino.png", "Top IPs de Destino")
        ]
        self.graficos_histograma = [
            ("img/histograms/tamanhos.png", "Distribuição dos Tamanhos dos Pacotes")
        ]
        self.graficos_pizza = [
            ("img/pizza/protocolos_pizza.png", "Proporção de Protocolos"),
            ("img/pizza/ip_origem_pizza.png", "Proporção dos Principais IPs de Origem"),
            ("img/pizza/ip_destino_pizza.png", "Proporção dos Principais IPs de Destino")
        ]
        self.graficos_heatmap = [
            ("img/heatmap/correlacao.png", "Mapa de Calor de Protocolos")

        ]
        self.graficos_tempo = [
            ("img/tempo/pacotes_tempo.png", "Pacotes por Tempo")
        ]

        # Inicializa a lista atual com uma lista vazia (será atribuída conforme o botão)
        self.graficos_atual = []
        self.indice_atual = 0  # Índice do gráfico atual

        layout_principal = BoxLayout(orientation='horizontal')

        # Barra lateral (25% da tela)
        barra_lateral = BoxLayout(orientation='vertical', size_hint=(0.25, 1), spacing=10, padding=10)

        label = Label(text="Tipos de Gráfico", size_hint=(1, 0.1))
        barra_lateral.add_widget(label)

        # Botões para alternar a exibição dos gráficos
        btn_barra = Button(text="Gráfico de Barra", size_hint=(1, 0.1))
        btn_barra.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_barra)

        btn_histograma = Button(text="Histograma", size_hint=(1, 0.1))
        btn_histograma.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_histograma)

        btn_pizza = Button(text="Gráfico de Pizza", size_hint=(1, 0.1))
        btn_pizza.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_pizza)

        btn_heatmap = Button(text="Mapa de Calor", size_hint=(1, 0.1))
        btn_heatmap.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_heatmap)

        btn_tempo = Button(text="Gráfico Temporal", size_hint=(1, 0.1))
        btn_tempo.bind(on_press=self.alternar_grafico)
        barra_lateral.add_widget(btn_tempo)

        # Botão de voltar
        btn_voltar = Button(text="Voltar", size_hint=(1, 0.1))
        btn_voltar.bind(on_press=self.voltar_menu)
        barra_lateral.add_widget(btn_voltar)

        layout_principal.add_widget(barra_lateral)

        # Área central (75% da tela) para o gráfico e explicação
        self.area_grafico = BoxLayout(orientation='vertical', size_hint=(0.75, 1), padding=20, spacing=10)

        # Fundo branco (ajustando para ocupar até 80% da altura)
        self.fundo_branco = FloatLayout(size_hint=(1.0, 0.8))
        with self.fundo_branco.canvas.before:
            Color(1, 1, 1, 1)  # cor do fundo branco
            self.fundo_rect = Rectangle(pos=self.fundo_branco.pos, size=self.fundo_branco.size)
            self.fundo_branco.bind(pos=self._update_fundo, size=self._update_fundo)

        # A imagem ocupa 80% do fundo branco e fica centralizada
        self.imagem = Image(
            size_hint=(0.8, 0.8),
            pos_hint={"center_x": 0.5, "center_y": 0.5},
            allow_stretch=True,
            keep_ratio=True
        )
        self.fundo_branco.add_widget(self.imagem)

        # Adicionar o fundo branco à área de gráficos
        self.area_grafico.add_widget(self.fundo_branco)

        # Label explicativo
        self.label_explicativo = Label(
            text="Legenda do gráfico aparecerá aqui",
            size_hint=(1, 0.1),
            halign='center'
        )
        self.area_grafico.add_widget(self.label_explicativo)

        # Botões de navegação
        box_nav = BoxLayout(size_hint=(1, 0.1), spacing=20, padding=(40, 0))
        btn_anterior = Button(text="Anterior")
        btn_proximo = Button(text="Próximo")
        btn_anterior.bind(on_press=self.anterior_grafico)
        btn_proximo.bind(on_press=self.proximo_grafico)
        box_nav.add_widget(btn_anterior)
        box_nav.add_widget(btn_proximo)
        self.area_grafico.add_widget(box_nav)

        layout_principal.add_widget(self.area_grafico)
        self.add_widget(layout_principal)

    def alternar_grafico(self, instance):
        # Dependendo do botão clicado, altera a lista de gráficos atual
        if instance.text == "Gráfico de Barra":
            self.graficos_atual = self.graficos_barras
        elif instance.text == "Histograma":
            self.graficos_atual = self.graficos_histograma
        elif instance.text == "Gráfico de Pizza":
            self.graficos_atual = self.graficos_pizza

        self.indice_atual = 0  # Reseta o índice ao selecionar um novo tipo de gráfico
        self.atualizar_imagem()

    def atualizar_imagem(self):
        if self.graficos_atual:  # Verifica se há gráficos na lista atual
            caminho_imagem, legenda = self.graficos_atual[self.indice_atual]
            if os.path.exists(caminho_imagem):
                self.imagem.source = caminho_imagem
                self.imagem.reload()
            self.label_explicativo.text = legenda

    def anterior_grafico(self, *args):
        if self.indice_atual > 0:
            self.indice_atual -= 1
            self.atualizar_imagem()

    def proximo_grafico(self, *args):
        if self.indice_atual < len(self.graficos_atual) - 1:
            self.indice_atual += 1
            self.atualizar_imagem()

    def voltar_menu(self, *args):
        self.manager.current = "Menu Inicial"

    def _update_fundo(self, instance, value):
        self.fundo_rect.pos = instance.pos
        self.fundo_rect.size = instance.size


class GerenciadorTelas(ScreenManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.add_widget(TelaMenuInicial(name="Menu Inicial"))
        self.add_widget(TelaGraficosBasicos(name="Graficos Basicos"))
        self.add_widget(TelaAnalise(name="Analise"))


class NetGraphicsApp(App):
    def build(self):
        return GerenciadorTelas()


if __name__ == "__main__":
    NetGraphicsApp().run()
