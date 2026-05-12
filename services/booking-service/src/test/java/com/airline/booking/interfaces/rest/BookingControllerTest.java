package com.airline.booking.interfaces.rest;

import com.airline.booking.application.dto.BookingDto;
import com.airline.booking.application.usecase.CreateBookingUseCase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BookingController.class)
class BookingControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CreateBookingUseCase createBookingUseCase;

    @Autowired
    private ObjectMapper objectMapper;

    private BookingDto validBookingDto;

    @BeforeEach
    void setUp() {
        validBookingDto = new BookingDto();
        validBookingDto.customerId = "CUST-1";
        validBookingDto.flightId = "FL-100";
        validBookingDto.passengers = 2;
        validBookingDto.usePoints = 50;
    }

    @Test
    void createBooking_shouldReturn202WithBookingId() throws Exception {
        // Given
        String expectedBookingId = "BKG-12345678";
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn(expectedBookingId);

        // When & Then
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test-123")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted())
                .andExpect(content().string(expectedBookingId));
    }

    @Test
    void createBooking_shouldUseProvidedCorrelationId() throws Exception {
        // Given
        String correlationId = "corr-custom-456";
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", correlationId)
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());

        // Then
        verify(createBookingUseCase).execute(any(BookingDto.class), argThat(corr -> corr.equals(correlationId)));
    }

    @Test
    void createBooking_shouldGenerateCorrelationIdIfMissing() throws Exception {
        // Given
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());

        // Then
        verify(createBookingUseCase).execute(any(BookingDto.class), argThat(corr -> corr.startsWith("corr-")));
    }

    @Test
    void createBooking_shouldGenerateCorrelationIdIfEmpty() throws Exception {
        // Given
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());

        // Then
        verify(createBookingUseCase).execute(any(BookingDto.class), argThat(corr -> corr.startsWith("corr-")));
    }

    @Test
    void createBooking_shouldAcceptValidBookingWithAllFields() throws Exception {
        // Given
        BookingDto dto = new BookingDto();
        dto.customerId = "CUST-999";
        dto.flightId = "FL-999";
        dto.seatClass = "BUSINESS";
        dto.passengers = 3;
        dto.usePoints = 1000;

        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-87654321");

        // When & Then
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test")
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isAccepted())
                .andExpect(content().string("BKG-87654321"));
    }

    @Test
    void createBooking_shouldAcceptBookingWithZeroPoints() throws Exception {
        // Given
        validBookingDto.usePoints = 0;
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When & Then
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());
    }

    @Test
    void createBooking_shouldHandleMultiplePassengers() throws Exception {
        // Given
        validBookingDto.passengers = 10;
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When & Then
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());

        verify(createBookingUseCase).execute(argThat(dto -> dto.passengers == 10), anyString());
    }

    @Test
    void createBooking_shouldReturnBookingIdInResponseBody() throws Exception {
        // Given
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-ABCD1234");

        // When & Then
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted())
                .andExpect(content().contentType("text/plain;charset=UTF-8"))
                .andExpect(content().string("BKG-ABCD1234"));
    }

    @Test
    void createBooking_shouldPassDtoToUseCase() throws Exception {
        // Given
        when(createBookingUseCase.execute(any(BookingDto.class), anyString()))
                .thenReturn("BKG-12345678");

        // When
        mockMvc.perform(post("/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "corr-test")
                        .content(objectMapper.writeValueAsString(validBookingDto)))
                .andExpect(status().isAccepted());

        // Then
        verify(createBookingUseCase).execute(
                argThat(dto ->
                        dto.customerId.equals("CUST-1") &&
                        dto.flightId.equals("FL-100") &&
                        dto.passengers == 2 &&
                        dto.usePoints == 50
                ),
                anyString()
        );
    }
}
